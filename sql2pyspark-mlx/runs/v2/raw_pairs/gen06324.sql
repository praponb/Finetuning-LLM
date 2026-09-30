SELECT customer_id FROM sessions WHERE started_at < '2025-08-27' UNION ALL SELECT customer_id FROM sessions WHERE country IN ('JP', 'SG', 'US')
