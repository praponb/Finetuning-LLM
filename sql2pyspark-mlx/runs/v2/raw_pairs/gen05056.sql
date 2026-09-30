SELECT c.country, COUNT(DISTINCT s.country) AS unique_countrys FROM sessions s RIGHT JOIN customers c ON s.customer_id = c.id WHERE s.started_at >= '2024-06-01' GROUP BY c.country;
