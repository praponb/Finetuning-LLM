SELECT COUNT(*) AS shipment_count FROM shipments WHERE carrier IN ('Flash', 'Kerry') ORDER BY shipment_count DESC;
