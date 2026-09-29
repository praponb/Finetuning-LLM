SELECT
    CASE WHEN cost >= 88 THEN 'high' WHEN cost >= 34 THEN 'mid' ELSE 'low' END AS cost_bucket,
    order_id,
    AVG(cost) AS avg_cost,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values
FROM shipments
GROUP BY 1, 2
HAVING AVG(cost) >= 58
ORDER BY cost_bucket
