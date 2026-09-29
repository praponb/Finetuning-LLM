SELECT monthly_fee
FROM subscriptions
WHERE end_date >= '2025-10-01'
EXCEPT
SELECT amount
FROM transactions
