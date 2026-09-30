SELECT shipped_date, CASE WHEN carrier IN ('UPS', 'FedEx') THEN 'Core' ELSE 'Other' END carrier_group, DATE_SUB(delivered_date, 43) delivered_date_minus_43d
FROM shipments
