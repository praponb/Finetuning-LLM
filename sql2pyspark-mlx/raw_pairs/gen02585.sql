WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(balance) OVER (PARTITION BY YEAR(opened_date), status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        FIRST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY customer_id, account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance,
        LAST_VALUE(balance, TRUE) OVER (PARTITION BY DATE_TRUNC('MONTH', opened_date) ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance
    FROM accounts
    WHERE account_type = 'CHECKING'
),
matched_keys AS (
    -- Set difference on keys
    SELECT status
    FROM scored
    EXCEPT DISTINCT
    SELECT status
    FROM accounts
    WHERE status IN ('OPEN', 'CLOSED', 'FROZEN')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.status = k.status
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM verified
    INTERSECT ALL
    SELECT id
    FROM accounts
    WHERE customer_id IN (10, 23)
),
verified_accounts AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN remaining_keys k ON s.id = k.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        opened_date,
        cumulative_balance,
        first_known_balance,
        last_known_balance,
        ROUND(cumulative_balance * 1.1, 2) AS cumulative_balance_adjusted,
        DATE_TRUNC('WEEK', opened_date) AS opened_date_period
    FROM verified_accounts
    WHERE opened_date >= DATE '2023-09-15'
      AND status IS DISTINCT FROM 'CLOSED'
)
SELECT
    s.customer_id,
    s.first_known_balance,
    s.cumulative_balance_adjusted,
    (SELECT MAX(t2.amount) FROM orders t2 WHERE t2.customer_id = s.customer_id) AS max_order_amount
FROM cleaned s
ORDER BY first_known_balance
