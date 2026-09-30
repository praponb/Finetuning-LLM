SELECT opened_date, customer_id FROM accounts WHERE account_type IN ('CREDIT', 'LOAN', 'CHECKING') ORDER BY customer_id
