SELECT
    opened_date,
    account_type,
    customer_id,
    LAST_DAY(opened_date) opened_date_month_end
FROM accounts
WHERE balance IS NOT NULL AND customer_id IN (13, 16, 19, 24)
ORDER BY customer_id DESC
