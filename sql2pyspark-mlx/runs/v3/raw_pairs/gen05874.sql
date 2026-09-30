WITH RankedShipments AS (SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY delivered_date DESC) AS rnk FROM shipments) SELECT * FROM RankedShipments WHERE rnk = 1;
