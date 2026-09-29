SELECT
    MONTH(a.opened_date) AS opened_month,
    a.status,
    MAX(t.amount) AS max_amount,
    ROUND(AVG(t.amount), 2) AS avg_amount
FROM transactions t
FULL OUTER JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
WHERE t.account_id = 17
GROUP BY MONTH(a.opened_date), a.status
ORDER BY avg_amount DESC;
