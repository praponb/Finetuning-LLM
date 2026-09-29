SELECT customer_id, MIN(opened_date) first_opened_date, COUNT(*) num_accounts FROM accounts WHERE status IN ('FROZEN', 'CLOSED') GROUP BY customer_id;
