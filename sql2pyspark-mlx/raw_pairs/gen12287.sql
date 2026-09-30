WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        status,
        AVG(balance) OVER (PARTITION BY status) AS status_avg_balance,
        ROW_NUMBER() OVER (PARTITION BY account_type, customer_id ORDER BY opened_date, id) AS row_seq,
        FIRST_VALUE(balance, TRUE) OVER w_hist AS first_known_balance,
        FIRST_VALUE(status, TRUE) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status
    FROM accounts
    WINDOW w_hist AS (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        status,
        status_avg_balance,
        row_seq,
        first_known_balance
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY first_known_balance, id) AS rank_in_group
        FROM with_windows
    ) ranked
    WHERE rank_in_group = 1
),
with_windows_accounts AS (
    -- Running and moving aggregates
    SELECT
        id,
        status,
        row_seq,
        LAG(status_avg_balance) OVER w_recent AS prev_status_avg_balance,
        LEAD(status_avg_balance) OVER w_recent AS next_status_avg_balance,
        LAG(first_known_balance) OVER w_recent AS prev_first_known_balance,
        LEAD(first_known_balance) OVER w_recent AS next_first_known_balance,
        ROW_NUMBER() OVER w_recent AS event_seq,
        LAG(status_avg_balance, 2) OVER (PARTITION BY status ORDER BY row_seq, id) AS prev_status_avg_balance_2
    FROM ranked
    WHERE customer_id IN (15, 25, 40)
    WINDOW w_recent AS (PARTITION BY status, customer_id ORDER BY first_known_balance, id)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.account_id IN (4, 14, 34)
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.merchant = 'Lazada'
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        row_seq,
        prev_status_avg_balance,
        prev_first_known_balance,
        next_first_known_balance,
        event_seq,
        prev_status_avg_balance_2,
        FIRST_VALUE(prev_first_known_balance, TRUE) OVER w_part AS first_known_prev_first_known_balance,
        CUME_DIST() OVER w_grp AS next_status_avg_balance_cume_dist
    FROM not_flagged
    WINDOW w_part AS (PARTITION BY status ORDER BY prev_status_avg_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY next_status_avg_balance NULLS FIRST)
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN prev_first_known_balance >= 62000 THEN 'HIGH' ELSE 'LOW' END AS prev_first_known_balance_tier,
        CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
        MIN(prev_status_avg_balance_2) AS min_prev_status_avg_balance_2,
        PERCENTILE_APPROX(prev_status_avg_balance, ARRAY(0.25, 0.5, 0.75)) AS prev_status_avg_balance_quartiles,
        MIN(prev_first_known_balance) AS min_prev_first_known_balance,
        APPROX_PERCENTILE(next_first_known_balance, ARRAY(0.25, 0.5, 0.75)) AS next_first_known_balance_quartiles,
        COUNT(*) AS cnt,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN prev_first_known_balance >= 62000 THEN 'HIGH' ELSE 'LOW' END ORDER BY MIN(prev_status_avg_balance_2) DESC) AS min_prev_status_avg_balance_2_dense_rank
    FROM scored
    GROUP BY CASE WHEN prev_first_known_balance >= 62000 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    prev_first_known_balance_tier,
    row_seq_tier,
    prev_status_avg_balance_quartiles,
    min_prev_first_known_balance,
    min_prev_status_avg_balance_2_dense_rank
FROM totals
ORDER BY row_seq_tier DESC;
