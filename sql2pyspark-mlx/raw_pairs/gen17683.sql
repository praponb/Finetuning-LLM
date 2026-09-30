WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(balance AS INT) AS balance_int
    FROM accounts
    WHERE opened_date > DATE '2023-09-14'
      AND balance > 26000
),
remaining_keys AS (
    -- Set difference on keys
    SELECT status, customer_id
    FROM base
    EXCEPT DISTINCT
    SELECT status, customer_id
    FROM accounts
    WHERE account_type = 'SAVINGS'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.customer_id = k.customer_id
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, d.city
    FROM reconciled a
    LEFT JOIN customers d
        ON a.customer_id = d.id
    WHERE LOWER(TRIM(d.name)) = 'jack taylor'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(balance) OVER (PARTITION BY MONTH(opened_date), account_type) AS opened_date_month_stddev_samp_balance,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY balance NULLS FIRST) AS balance_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY balance DESC) AS balance_percent_rank
    FROM joined
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time >= s.opened_date
        )
)
SELECT
    id,
    account_type,
    balance,
    opened_date,
    status,
    balance_int,
    opened_date_month_stddev_samp_balance,
    balance_dense_rank,
    balance_percent_rank,
    CASE WHEN status RLIKE '^(OPEN|FROZEN)$' THEN 'GROUP_B' WHEN status RLIKE '^(CLOSED)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS status_zone
FROM not_flagged
ORDER BY account_type
