SELECT id, customer_id FROM sessions WHERE customer_id IS NOT NULL EXCEPT SELECT id, customer_id FROM sessions WHERE country IN ('TH', 'UK', 'DE')
