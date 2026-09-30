WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(account_type, 1, 4) AS account_type_prefix
    FROM accounts
    WHERE balance BETWEEN 41000 AND 76000
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS seq_no,
        FIRST_VALUE(status, TRUE) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status
    FROM valid
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance DESC NULLS LAST, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(balance) OVER w_running AS balance_avg_2_prior,
        SUM(seq_no) OVER (PARTITION BY status, account_type) AS status_sum_seq_no
    FROM eligible
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance_avg_2_prior, id) = 1
)
SELECT
    s.id,
    s.customer_id,
    s.opened_date,
    s.seq_no,
    s.first_known_status,
    s.status_sum_seq_no,
    (SELECT SUM(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS sum_account_balance
FROM latest s
ORDER BY id DESC;
