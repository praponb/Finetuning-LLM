SELECT a.id, a.opened_date FROM accounts a WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE country IN ('UK', 'US', 'TH'))
