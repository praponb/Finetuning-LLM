SELECT country, customer_id, started_at, DATE_FORMAT(started_at, 'yyyy-MM') AS started_at_label FROM sessions WHERE device = 'mobile';
