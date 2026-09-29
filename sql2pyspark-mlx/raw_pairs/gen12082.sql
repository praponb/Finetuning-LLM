SELECT s.id, s.started_at, c.age FROM sessions s JOIN customers c ON s.customer_id = c.id WHERE s.customer_id IN (2, 13) AND s.started_at >= '2023-12-30';
