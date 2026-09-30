SELECT customer_id, opened_date FROM accounts WHERE balance >= 24000 INTERSECT SELECT customer_id, opened_date FROM accounts WHERE account_type = 'CREDIT' ORDER BY customer_id DESC;
