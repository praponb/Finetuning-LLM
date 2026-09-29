SELECT
    id,
    customer_id,
    account_type,
    opened_date,
    QUARTER(opened_date) AS opened_date_quarter
FROM accounts
WHERE opened_date BETWEEN '2023-03-01' AND '2024-07-01' AND status = 'OPEN'
