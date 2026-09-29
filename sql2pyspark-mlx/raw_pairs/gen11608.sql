WITH filtered AS (
    SELECT
        *,
        LOWER(carrier) AS carrier_lower,
        COALESCE(carrier, 'DIRECT') AS resolved_carrier
    FROM shipments
    WHERE carrier IS NOT DISTINCT FROM 'Kerry'
      AND carrier IS NOT NULL
)
SELECT
    dim.channel,
    dim.status AS order_status,
    COUNT(*) AS rows_total,
    MIN(src.cost) AS min_cost
FROM filtered src
LEFT JOIN orders dim ON src.order_id = dim.id
GROUP BY GROUPING SETS ((dim.channel, dim.status), (dim.channel), ())
ORDER BY min_cost NULLS FIRST
