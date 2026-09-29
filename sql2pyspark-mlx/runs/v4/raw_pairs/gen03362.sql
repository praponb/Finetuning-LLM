WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label
    FROM accounts
    WHERE opened_date > DATE '2023-07-06'
      AND customer_id IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(balance) OVER (ORDER BY balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        AVG(balance) OVER (PARTITION BY account_type) AS account_type_avg_balance
    FROM valid
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        opened_date_label,
        cumulative_balance,
        account_type_avg_balance,
        ROUND(cumulative_balance * 1.15, 2) AS adjusted_cumulative_balance,
        cumulative_balance / NULLIF(balance, 0) AS cumulative_balance_per_balance
    FROM scored
    WHERE opened_date >= DATE '2024-04-09'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.credit_limit,
        c.email,
        c.age
    FROM filtered e
    LEFT JOIN customers c
        ON e.customer_id = c.id
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    CASE WHEN account_type_avg_balance >= 65172 THEN 'PREMIUM' ELSE 'STANDARD' END AS account_type_avg_balance_tier,
    ROUND(AVG(balance), 2) AS avg_balance,
    MAX(credit_limit) FILTER (WHERE account_type = 'LOAN') AS loan_max_credit_limit,
    AVG(cumulative_balance) FILTER (WHERE account_type = 'SAVINGS') AS savings_avg_cumulative_balance
FROM with_dims
GROUP BY customer_id, CASE WHEN account_type_avg_balance >= 65172 THEN 'PREMIUM' ELSE 'STANDARD' END
