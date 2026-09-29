WITH rollup_totals AS (
    -- Join a dimension and aggregate
    SELECT
        p.region,
        p.city,
        COUNT(*) AS num_rows,
        ROUND(AVG(s.shipping_fee), 2) AS avg_shipping_fee,
        ROUND(SUM(s.amount), 2) AS total_amount,
        GROUPING_ID() AS gid
    FROM orders s
    INNER JOIN stores p ON s.store_id = p.id
    GROUP BY GROUPING SETS ((p.region, p.city), (p.region), ())
)
SELECT
    city,
    num_rows,
    total_amount
FROM rollup_totals
ORDER BY total_amount DESC NULLS FIRST
