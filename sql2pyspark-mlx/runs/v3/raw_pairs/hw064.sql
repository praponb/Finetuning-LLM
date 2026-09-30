SELECT id, DATEDIFF(delivered_date, shipped_date) AS transit_days FROM shipments;
