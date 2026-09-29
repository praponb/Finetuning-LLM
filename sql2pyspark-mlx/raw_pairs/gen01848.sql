SELECT
    id,
    status,
    balance,
    DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label,
    DATE_TRUNC('WEEK', opened_date) AS opened_date_period
FROM accounts
WHERE status IN ('OPEN', 'CLOSED', 'FROZEN');
