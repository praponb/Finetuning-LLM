SELECT order_id, MAX(shipped_date) AS latest_shipped_date, COUNT(DISTINCT cost) AS unique_statuss FROM shipments GROUP BY 1;
