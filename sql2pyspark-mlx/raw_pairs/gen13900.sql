SELECT c.city, COUNT(*) AS cnt FROM sessions s FULL OUTER JOIN customers c ON s.customer_id = c.id GROUP BY c.city ORDER BY 2 DESC;
