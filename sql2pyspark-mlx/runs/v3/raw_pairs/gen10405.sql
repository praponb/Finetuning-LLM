SELECT carrier, CASE WHEN carrier IN ('FedEx', 'Kerry') THEN 'Core' ELSE 'Other' END carrier_group FROM shipments WHERE status = 'DELIVERED' ORDER BY carrier, carrier_group DESC;
