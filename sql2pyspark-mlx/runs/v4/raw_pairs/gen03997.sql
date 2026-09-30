WITH cleaned AS (
    SELECT *, CASE WHEN cost >= 97 THEN 'High' WHEN cost >= 46 THEN 'Medium' ELSE 'Low' END AS cost_band
    FROM shipments
    WHERE order_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        carrier,
        status,
        cost
    FROM cleaned
) src
PIVOT (
    AVG(cost)
    FOR status IN ('LOST', 'IN_TRANSIT')
);
