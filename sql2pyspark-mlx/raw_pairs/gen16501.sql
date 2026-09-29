WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN status IN ('DELIVERED', 'LOST') THEN 'Priority' ELSE 'Other' END AS status_group,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM shipments
    WHERE carrier IS DISTINCT FROM 'Kerry'
      AND carrier = 'Kerry'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        carrier,
        status,
        cost
    FROM filtered
) src
PIVOT (
    MAX(cost) AS total,
    COUNT(1) AS cnt
    FOR status IN ('RETURNED', 'LOST', 'DELIVERED')
);
