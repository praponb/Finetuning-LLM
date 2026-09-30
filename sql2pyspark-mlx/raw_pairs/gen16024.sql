WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) AS row_num
    FROM shipments
    QUALIFY row_num = 1
)
-- Grouped metrics
SELECT
    carrier,
    COUNT(CASE WHEN carrier = 'Kerry' THEN 1 END) AS kerry_count,
    SUM(CASE WHEN status = 'RETURNED' THEN cost ELSE 0 END) AS returned_cost,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles
FROM ranked
GROUP BY carrier
HAVING kerry_count > 1
ORDER BY returned_cost DESC
