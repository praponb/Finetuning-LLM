SELECT *
FROM (
    SELECT
        DATE_FORMAT(delivered_date, 'yyyy-MM') delivered_month,
        status,
        SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) delivered_count,
        SUM(cost) total_cost
    FROM shipments
    GROUP BY DATE_FORMAT(delivered_date, 'yyyy-MM'), status
) agg
WHERE total_cost >= 520
ORDER BY total_cost;
