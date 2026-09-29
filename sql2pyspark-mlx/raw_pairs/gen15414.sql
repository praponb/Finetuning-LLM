WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        TRIM(carrier) AS clean_carrier,
        COALESCE(carrier, 'NONE') AS carrier_clean
    FROM shipments
    WHERE delivered_date > DATE '2023-10-02'
      AND order_id IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        status,
        cost
    FROM valid
) src
PIVOT (
    SUM(cost)
    FOR status IN ('LOST' AS lost, 'RETURNED' AS returned, 'DELIVERED' AS delivered)
);
