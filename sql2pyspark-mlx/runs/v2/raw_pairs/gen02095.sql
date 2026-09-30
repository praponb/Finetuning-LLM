SELECT id, device, country, customer_id, DATE_FORMAT(started_at, 'yyyy-MM') started_at_label FROM sessions WHERE country IN ('SG', 'JP');
