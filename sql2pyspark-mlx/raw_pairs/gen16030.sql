SELECT
    c.segment,
    c.city,
    COUNT(CASE WHEN a.account_type = 'CREDIT' THEN 1 END) AS credit_count,
    COUNT(a.balance) AS balance_count
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.customer_id IN (2, 21, 26, 36)
GROUP BY c.segment, c.city
ORDER BY credit_count DESC
