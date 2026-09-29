SELECT
    customer_id,
    status,
    opened_date,
    DATE_TRUNC('year', opened_date) AS opened_date_period,
    CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END AS is_closed
FROM accounts
WHERE customer_id = 15 AND customer_id IS NOT NULL AND account_type IN ('SAVINGS', 'CHECKING')
ORDER BY is_closed;
