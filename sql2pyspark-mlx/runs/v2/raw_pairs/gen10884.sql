SELECT YEAR(c.signup_date) AS signup_year, SUM(t.amount) AS total_amount, SUM(CASE WHEN t.merchant = 'Lazada' THEN 1 ELSE 0 END) AS lazada_count
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON c.id = a.customer_id
GROUP BY YEAR(c.signup_date);
