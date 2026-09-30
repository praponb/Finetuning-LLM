SELECT id, customer_id, device FROM sessions WHERE country = 'UK' EXCEPT SELECT id, customer_id, device FROM sessions WHERE country IN ('JP', 'UK', 'DE') ORDER BY customer_id;
