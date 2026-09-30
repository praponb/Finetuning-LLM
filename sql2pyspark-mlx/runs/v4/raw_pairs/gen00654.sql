WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        CASE WHEN carrier IN ('UPS', 'DHL') THEN 'Group A' ELSE 'Other' END AS carrier_group,
        CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END AS is_delivered
    FROM shipments
    WHERE status = 'RETURNED'
)
SELECT status
FROM base
INTERSECT
SELECT status
FROM shipments
WHERE shipped_date > '2023-07-01';
