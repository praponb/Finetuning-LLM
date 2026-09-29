WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        status,
        DATEDIFF(CURRENT_DATE(), opened_date) days_since_opened_date
    FROM accounts
    WHERE customer_id IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        days_since_opened_date,
        SUM(days_since_opened_date) OVER (PARTITION BY status) status_sum_days_since_opened_date,
        SUM(days_since_opened_date) OVER (PARTITION BY customer_id ORDER BY days_since_opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_days_since_opened_date,
        NTILE(5) OVER w_hist days_since_opened_date_bucket,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY days_since_opened_date DESC, id) row_seq,
        SUM(days_since_opened_date) OVER (PARTITION BY status, customer_id ORDER BY days_since_opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_days_since_opened_date_2
    FROM filtered
    WINDOW w_hist AS (PARTITION BY status ORDER BY days_since_opened_date, id)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM enriched
    EXCEPT ALL
    SELECT id, customer_id
    FROM accounts
    WHERE account_type = 'SAVINGS'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_type = 'CREDIT'
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_type = 'REFUND'
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, STDDEV_SAMP(row_seq) OVER (PARTITION BY customer_id) customer_id_stddev_samp_row_seq
    FROM not_flagged
)
SELECT
    s.customer_id,
    s.days_since_opened_date,
    s.status_sum_days_since_opened_date,
    s.row_seq,
    s.cumulative_days_since_opened_date_2,
    s.customer_id_stddev_samp_row_seq,
    (SELECT MAX(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS max_order_amount
FROM metrics s
ORDER BY status_sum_days_since_opened_date;
