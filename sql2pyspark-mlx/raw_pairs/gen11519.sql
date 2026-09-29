SELECT country, customer_id, MONTH(started_at) started_at_month FROM sessions WHERE customer_id IS NOT NULL OR browser = 'Chrome' ORDER BY started_at_month DESC
