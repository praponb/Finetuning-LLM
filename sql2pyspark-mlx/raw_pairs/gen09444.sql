SELECT
    id,
    status,
    start_date,
    CEIL(monthly_fee) AS monthly_fee_whole
FROM subscriptions
WHERE customer_id = 12 AND start_date BETWEEN '2023-04-04' AND '2025-03-01'
ORDER BY start_date ASC, id ASC
LIMIT 3
