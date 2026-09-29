SELECT browser, started_at, customer_id, DATE_FORMAT(started_at, 'yyyy-MM-dd') AS started_at_label, COALESCE(country, 'N/A') AS country_filled FROM sessions;
