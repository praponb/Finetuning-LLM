WITH Numbered AS (SELECT id, carrier, cost, status, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS pos FROM shipments WHERE carrier = 'DHL') SELECT * FROM Numbered WHERE pos <= 2;
