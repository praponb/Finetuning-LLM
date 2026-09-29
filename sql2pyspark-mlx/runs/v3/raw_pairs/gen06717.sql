SELECT
    customer_id,
    opened_date,
    balance,
    DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label,
    CONCAT(status, ' ', account_type) AS status_account_type
FROM accounts
WHERE status = 'OPEN' AND customer_id IS NULL AND balance < 27000;
