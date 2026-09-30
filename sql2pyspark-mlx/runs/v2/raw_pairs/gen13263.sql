SELECT customer_id, opened_date FROM accounts WHERE balance <= 30000 UNION SELECT customer_id, opened_date FROM accounts WHERE status = 'FROZEN' ORDER BY opened_date
