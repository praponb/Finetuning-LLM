SELECT c.segment, COUNT(DISTINCT s.country) AS unique_countrys FROM sessions s JOIN customers c ON s.customer_id = c.id GROUP BY c.segment ORDER BY segment
