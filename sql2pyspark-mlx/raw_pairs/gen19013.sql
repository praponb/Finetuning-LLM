WITH metrics_long AS (
    -- Native SQL UNPIVOT (wide to long)
    SELECT
        id,
        channel,
        metric,
        val
    FROM (
        SELECT
            id,
            channel,
            discount,
            amount,
            shipping_fee
        FROM orders
    ) src
    UNPIVOT (
        val FOR metric IN (discount AS DISCOUNT_VAL, amount AS AMOUNT_VAL, shipping_fee AS SHIPPING_FEE_VAL)
    ) u
)
-- Multi-level aggregation
SELECT
    metric,
    CASE WHEN val >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_tier,
    channel,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    COUNT(*) AS event_count
FROM metrics_long
GROUP BY CUBE (metric, CASE WHEN val >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END, channel)
ORDER BY event_count DESC NULLS LAST;
