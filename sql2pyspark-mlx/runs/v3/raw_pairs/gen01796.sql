WITH ranked_shipments AS (SELECT *, RANK() OVER (PARTITION BY carrier ORDER BY cost DESC) AS pos FROM shipments WHERE delivered_date > '2025-06-22') SELECT * FROM ranked_shipments WHERE pos <= 3;
