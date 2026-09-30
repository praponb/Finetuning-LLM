SELECT
    status,
    order_id,
    CASE WHEN status IN ('IN_TRANSIT', 'RETURNED') THEN 'Priority' ELSE 'Other' END AS status_group,
    ROUND(cost, 1) AS cost_rounded
FROM shipments
WHERE delivered_date BETWEEN '2023-09-27' AND '2024-06-01' AND cost <= 49
ORDER BY 3, order_id
