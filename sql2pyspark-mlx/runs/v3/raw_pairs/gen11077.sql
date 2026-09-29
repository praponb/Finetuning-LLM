SELECT
    CASE WHEN status IN ('LOST', 'IN_TRANSIT') THEN 'group_a' ELSE 'other' END AS status_group,
    SUM(cost) AS total_cost,
    COUNT(*) AS cnt,
    SUM(SUM(cost)) OVER (ORDER BY CASE WHEN status IN ('LOST', 'IN_TRANSIT') THEN 'group_a' ELSE 'other' END) AS cumulative_cost
FROM shipments
GROUP BY CASE WHEN status IN ('LOST', 'IN_TRANSIT') THEN 'group_a' ELSE 'other' END
