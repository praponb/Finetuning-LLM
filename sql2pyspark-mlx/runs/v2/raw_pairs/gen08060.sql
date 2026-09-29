SELECT plan, start_date
FROM subscriptions
WHERE customer_id IS NULL AND start_date BETWEEN '2023-08-02' AND '2024-08-05'
ORDER BY plan;
