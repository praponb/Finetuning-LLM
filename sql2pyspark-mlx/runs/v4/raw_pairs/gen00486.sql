WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(account_type, '/', status) AS account_type_status,
        DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label,
        COALESCE(status, 'UNKNOWN') AS status_filled
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND status = 'OPEN'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT customer_id, account_type
    FROM base
    INTERSECT
    SELECT customer_id, account_type
    FROM accounts
    WHERE balance <= 77000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.account_type = k.account_type
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.merchant IN ('Shell', 'Lazada')
        )
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        account_type_status,
        status_filled,
        INITCAP(TRIM(account_type)) AS account_type_title
    FROM not_flagged
    WHERE opened_date >= TO_DATE('2023-06-19')
      AND YEAR(opened_date) = 2023
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.account_id IN (1, 4, 10, 26)
        )
)
-- Multi-level aggregation
SELECT
    account_type,
    customer_id,
    SUM(balance) FILTER (WHERE account_type = 'SAVINGS') AS savings_total_balance,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
    COUNT(CASE WHEN account_type = 'CREDIT' THEN 1 END) AS credit_count,
    MIN(balance) AS min_balance,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles_2
FROM eligible
GROUP BY account_type, customer_id
HAVING credit_count > 1
ORDER BY customer_id DESC
