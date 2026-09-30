SELECT id, start_date
FROM subscriptions
WHERE start_date BETWEEN '2023-07-01' AND '2024-11-09' AND start_date <= '2024-08-06' AND end_date BETWEEN '2024-11-18' AND '2025-02-01'
