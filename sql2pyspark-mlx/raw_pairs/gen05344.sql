WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        shipped_date,
        cost,
        status,
        DATE_SUB(shipped_date, 3) AS shipped_date_minus_3d,
        DATEDIFF(delivered_date, shipped_date) / 7 AS weeks_shipped_date_to_delivered_date
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-03-12')
      AND order_id = 22
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        order_id,
        status,
        weeks_shipped_date_to_delivered_date
    FROM filtered
) src
PIVOT (
    SUM(weeks_shipped_date_to_delivered_date)
    FOR status IN ('DELIVERED', 'LOST')
)
