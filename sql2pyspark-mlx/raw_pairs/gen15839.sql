WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label,
        COALESCE(status, 'OTHER') AS status_clean
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-05-29')
      AND balance < 47000
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT ALL
    SELECT id
    FROM accounts
    WHERE status = 'FROZEN'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.id = k.id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.amount >= 1900
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id, status
    FROM kept
    EXCEPT DISTINCT
    SELECT customer_id, status
    FROM accounts
    WHERE account_type = 'CHECKING'
),
kept_rows_accounts AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    JOIN kept_keys k ON s.customer_id = k.customer_id AND s.status = k.status
),
base_accounts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        balance,
        opened_date,
        opened_date_label,
        status_clean,
        DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_date_label_2,
        LAST_DAY(opened_date) AS opened_date_month_end
    FROM kept_rows_accounts
    WHERE opened_date >= DATE '2023-03-23'
      AND status IN ('CLOSED', 'FROZEN')
),
kept_accounts AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.account_id IN (10, 18, 24)
        )
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', opened_date_month_end) AS activity_month,
        ROUND(AVG(balance), 2) AS avg_balance,
        PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
        COUNT(*) AS row_count
    FROM kept_accounts
    GROUP BY 1
    HAVING ROUND(AVG(balance), 2) > 0
)
SELECT activity_month, balance_quartiles
FROM daily
ORDER BY activity_month DESC NULLS LAST
