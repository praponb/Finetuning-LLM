WITH RankedShipments AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC, id) pos FROM shipments) SELECT * FROM RankedShipments WHERE pos <= 3;
