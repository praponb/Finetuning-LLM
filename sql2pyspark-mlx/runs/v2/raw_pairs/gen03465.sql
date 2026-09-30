SELECT
    opened_date,
    customer_id,
    status,
    LAST_DAY(opened_date) AS opened_date_month_end
FROM accounts
WHERE (customer_id = 23 AND balance >= 41000) OR balance <= 58000
ORDER BY customer_id;
