SELECT * FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY browser ORDER BY started_at DESC) AS rnk FROM sessions) AS ranked WHERE rnk <= 5 ORDER BY country, customer_id
