WITH matched_keys AS (
    -- Set difference on keys
    SELECT account_type, id
    FROM accounts
    EXCEPT
    SELECT account_type, id
    FROM accounts
    WHERE opened_date < '2024-09-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM accounts s
    INNER JOIN matched_keys k ON s.account_type = k.account_type AND s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN status = 'CLOSED' THEN 1 END), 0), 2) AS balance_per_closed,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN status = 'CLOSED' THEN 1 END), 0), 2) AS balance_per_closed_2,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN status = 'OPEN' THEN 1 END), 0), 2) AS balance_per_open,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(*) FILTER (WHERE account_type = 'SAVINGS') AS savings_rows
FROM verified
GROUP BY 1
ORDER BY balance_per_closed_2
