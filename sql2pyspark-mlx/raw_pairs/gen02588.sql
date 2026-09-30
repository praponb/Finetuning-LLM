WITH base AS (
    SELECT *, FLOOR(balance) balance_whole
    FROM accounts
    WHERE customer_id IS NULL AND opened_date > '2024-06-01'
),
AccountsStats AS (
    SELECT MONTH(opened_date) opened_month, ROUND(AVG(balance), 2) avg_balance
    FROM base
    GROUP BY MONTH(opened_date)
)
SELECT *
FROM AccountsStats
WHERE avg_balance > 56
ORDER BY opened_month DESC, 2
