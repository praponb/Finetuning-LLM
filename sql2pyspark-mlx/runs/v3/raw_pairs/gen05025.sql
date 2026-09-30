SELECT
    order_id,
    status,
    carrier,
    CASE WHEN carrier IN ('Kerry', 'DHL') THEN 'Tier 1' ELSE 'Other' END AS carrier_group,
    TRIM(carrier) AS clean_carrier
FROM shipments;
