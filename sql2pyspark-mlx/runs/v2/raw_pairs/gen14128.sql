SELECT id, monthly_fee
FROM subscriptions
WHERE end_date BETWEEN '2023-08-01' AND '2024-02-01'
ORDER BY id DESC;
