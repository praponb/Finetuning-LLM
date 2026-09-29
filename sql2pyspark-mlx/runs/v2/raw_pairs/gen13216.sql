SELECT customer_id, country, DATE_FORMAT(started_at, 'MMM yyyy') AS started_at_label FROM sessions WHERE started_at < '2024-10-01';
