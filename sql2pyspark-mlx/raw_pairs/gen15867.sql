WITH rollup_totals AS (
    SELECT
        o.status,
        COUNT(*) AS cnt,
        ROUND(SUM(e.unit_price), 2) AS total_unit_price,
        GROUPING_ID() AS level_id
    FROM order_items e
    LEFT JOIN orders o ON e.order_id = o.id
    GROUP BY ROLLUP (o.status)
)
SELECT
    status,
    total_unit_price,
    level_id,
    CASE WHEN status RLIKE '^(PENDING|RETURNED)$' THEN 'PRIORITY' WHEN status RLIKE '^(COMPLETED|CANCELLED)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS status_zone
FROM rollup_totals;
