SELECT
    id,
    status,
    balance,
    customer_id,
    ROUND(balance * 0.9, 2) AS balance_scaled,
    CASE WHEN balance >= 58000 THEN 'High' WHEN balance >= 27000 THEN 'Medium' ELSE 'Low' END AS balance_tier
FROM accounts
WHERE opened_date < '2024-04-01'
ORDER BY balance_tier
