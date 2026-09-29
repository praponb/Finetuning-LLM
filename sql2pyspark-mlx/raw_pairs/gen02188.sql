WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(carrier) AS carrier_upper,
        COALESCE(carrier, 'DIRECT') AS resolved_carrier
    FROM shipments
    WHERE order_id IS NOT NULL
)
SELECT order_id, id
FROM base
INTERSECT
SELECT order_id, id
FROM shipments
WHERE carrier IN ('DHL', 'UPS', 'Flash');
