SELECT *
FROM accounts
WHERE MONTH(opened_date) IN (4, 6, 12)
ORDER BY customer_id DESC;
