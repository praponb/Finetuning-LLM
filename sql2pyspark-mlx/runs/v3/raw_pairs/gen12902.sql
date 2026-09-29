SELECT a.status, ROUND(STDDEV(t.amount), 2) AS stddev_amount
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
FULL OUTER JOIN customers c ON a.customer_id = c.id
WHERE LENGTH(c.name) > 11
GROUP BY a.status
ORDER BY stddev_amount DESC;
