SELECT customer_id, id FROM accounts INTERSECT SELECT customer_id, id FROM accounts WHERE balance BETWEEN 30000 AND 55000
