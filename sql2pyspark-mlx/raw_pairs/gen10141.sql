SELECT id, carrier, COUNT(*) OVER (PARTITION BY carrier) AS carrier_count FROM shipments;
