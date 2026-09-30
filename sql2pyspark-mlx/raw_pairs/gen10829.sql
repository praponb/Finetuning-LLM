WITH metrics_long AS (
    SELECT *
    FROM (
        SELECT
            id,
            channel,
            amount,
            shipping_fee
        FROM orders
    ) src
    UNPIVOT (
        val FOR metric IN (amount AS AMOUNT, shipping_fee)
    ) unp
)
SELECT
    metric,
    CASE WHEN val >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_tier,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    COUNT_IF(metric = 'shipping_fee') AS shipping_fee_events,
    SORT_ARRAY(COLLECT_SET(metric)) AS metric_list,
    COUNT(*) FILTER (WHERE metric = 'shipping_fee') AS shipping_fee_rows,
    SUM(CASE WHEN metric = 'shipping_fee' THEN val ELSE 0 END) AS shipping_fee_val,
    GROUPING(metric) AS grouping_level
FROM metrics_long
GROUP BY GROUPING SETS ((metric, CASE WHEN val >= 69 THEN 'PREMIUM' ELSE 'STANDARD' END), (metric));
