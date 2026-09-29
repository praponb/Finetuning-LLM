WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        CEIL(balance) balance_whole,
        COALESCE(status, 'UNKNOWN') status_clean
    FROM accounts
    WHERE status IS NOT NULL
      AND status IN ('OPEN', 'CLOSED', 'FROZEN')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(balance_whole, TRUE) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_balance_whole,
        FIRST_VALUE(status, TRUE) OVER (PARTITION BY MONTH(opened_date), status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_status,
        MAX(balance) OVER (PARTITION BY customer_id) customer_id_max_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) event_seq,
        FIRST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_balance
    FROM valid
    WHERE balance <= 69000
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY opened_date DESC, id) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time >= s.opened_date
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY balance_whole, id) balance_whole_bucket,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('YEAR', opened_date) ORDER BY customer_id_max_balance DESC NULLS LAST) customer_id_max_balance_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY MONTH(opened_date) ORDER BY dedup_rank, id) row_seq,
        AVG(last_known_balance_whole) OVER w_hist last_known_balance_whole_avg_2
    FROM kept
    WINDOW w_hist AS (PARTITION BY MONTH(opened_date), account_type ORDER BY customer_id_max_balance, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.status,
        src.status_clean,
        src.last_known_balance_whole,
        src.event_seq,
        src.first_known_balance,
        c.city,
        c.signup_date
    FROM metrics src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.account_id IN (10, 12)
        )
)
-- Join a dimension and aggregate
SELECT
    dim.city AS customer_city,
    a.city AS account_city,
    COUNT(*) AS num_rows,
    AVG(a.event_seq) AS avg_event_seq
FROM eligible a
INNER JOIN customers dim ON a.customer_id = dim.id
GROUP BY dim.city, a.city;
