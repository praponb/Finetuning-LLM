SELECT DATE_FORMAT(a.opened_date, 'yyyy-MM') AS opened_month, AVG(t.amount) AS avg_amount
FROM transactions t
LEFT JOIN accounts a ON a.id = t.account_id
LEFT JOIN customers c ON c.id = a.customer_id
GROUP BY DATE_FORMAT(a.opened_date, 'yyyy-MM')
ORDER BY 1
