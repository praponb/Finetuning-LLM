SELECT c.city, ROUND(STDDEV(a.balance), 2) AS stddev_balance
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
GROUP BY c.city;
