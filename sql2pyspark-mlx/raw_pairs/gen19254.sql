SELECT customer_id FROM sessions WHERE country IN ('UK', 'DE', 'US') EXCEPT SELECT customer_id FROM sessions WHERE started_at < '2025-06-21';
