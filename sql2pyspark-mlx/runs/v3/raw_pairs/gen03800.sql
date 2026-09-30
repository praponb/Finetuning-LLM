SELECT id, start_date
FROM subscriptions
WHERE start_date <= '2023-08-01' AND monthly_fee > 70
ORDER BY start_date DESC, id
LIMIT 3;
