SELECT
    carrier,
    status,
    CASE WHEN carrier IN ('DHL', 'UPS') THEN 'priority' ELSE 'other' END AS carrier_group,
    COUNT(*) AS num_shipments,
    SUM(CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END) AS kerry_count,
    MAX(delivered_date) AS latest_delivered_date
FROM shipments
GROUP BY carrier, status, CASE WHEN carrier IN ('DHL', 'UPS') THEN 'priority' ELSE 'other' END
ORDER BY carrier_group;
