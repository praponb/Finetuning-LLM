SELECT DATE_TRUNC('MONTH', a.opened_date) AS opened_month, a.customer_id, COUNT(CASE WHEN t.merchant = 'Grab' THEN 1 END) AS grab_count
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
GROUP BY DATE_TRUNC('MONTH', a.opened_date), a.customer_id
ORDER BY 3, opened_month;
