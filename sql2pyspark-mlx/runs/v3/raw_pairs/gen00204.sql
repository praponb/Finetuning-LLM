SELECT status
FROM accounts
EXCEPT
SELECT status
FROM subscriptions
WHERE start_date BETWEEN '2023-07-01' AND '2023-09-01';
