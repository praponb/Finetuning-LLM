SELECT
    MONTH(opened_date) AS opened_month,
    account_type,
    SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_count,
    MAX(opened_date) AS last_opened_date
FROM accounts
WHERE customer_id IN (11, 17, 26)
GROUP BY 1, 2
HAVING open_count < 29;
