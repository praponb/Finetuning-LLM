SELECT customer_id, opened_date, account_type
FROM accounts
WHERE balance BETWEEN 69000 AND 71000 AND YEAR(opened_date) >= 2023 AND balance BETWEEN 28000 AND 75000
ORDER BY customer_id DESC;
