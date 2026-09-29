SELECT id, country, customer_id, started_at FROM sessions WHERE NOT (browser IN ('Safari', 'Firefox', 'Edge') AND country NOT IN ('SG', 'DE', 'TH'))
