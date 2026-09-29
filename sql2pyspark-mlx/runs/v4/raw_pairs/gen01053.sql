SELECT id, country, customer_id FROM sessions WHERE TO_DATE(started_at) >= '2024-05-01' UNION ALL SELECT id, country, customer_id FROM sessions WHERE country NOT IN ('US', 'DE');
