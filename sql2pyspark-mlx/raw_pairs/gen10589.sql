SELECT c.city, c.country, t.balance_range
FROM customers c
LEFT JOIN (SELECT customer_id, MAX(balance) - MIN(balance) AS balance_range
    FROM accounts
    WHERE customer_id IN (2, 4, 7, 30)
    GROUP BY customer_id) t ON c.id = t.customer_id
WHERE t.balance_range > 24
