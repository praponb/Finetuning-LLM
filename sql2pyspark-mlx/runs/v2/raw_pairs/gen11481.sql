WITH top_shipments AS (SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY cost) AS rnk FROM shipments WHERE MONTH(shipped_date) IN (5, 7, 11)) SELECT * FROM top_shipments WHERE rnk <= 3;
