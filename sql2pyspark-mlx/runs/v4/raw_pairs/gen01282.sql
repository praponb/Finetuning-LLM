SELECT opened_date, customer_id, balance
FROM accounts
WHERE balance IS NOT NULL
