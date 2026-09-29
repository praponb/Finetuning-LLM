SELECT end_date
FROM subscriptions
EXCEPT
SELECT opened_date end_date
FROM accounts;
