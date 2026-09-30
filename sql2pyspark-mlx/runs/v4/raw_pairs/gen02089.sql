WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CASE WHEN quantity >= 7 THEN 'High' WHEN quantity >= 2 THEN 'Medium' ELSE 'Low' END quantity_band
    FROM order_items
    WHERE product_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY unit_price ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) quantity_avg_2,
        CORR(unit_price, quantity) OVER (PARTITION BY product_id) corr_unit_price_quantity,
        SUM(unit_price) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_unit_price,
        LEAD(unit_price, 1) OVER (ORDER BY quantity ASC, id ASC) next_unit_price
    FROM cleaned
),
enriched_order_items AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.shipping_fee,
        b.channel,
        b.amount
    FROM enriched src
    LEFT JOIN orders b
        ON src.order_id = b.id
)
-- Grouped metrics
SELECT
    CASE WHEN next_unit_price >= 430 THEN 'LARGE' ELSE 'SMALL' END AS next_unit_price_tier,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
    PERCENTILE_APPROX(shipping_fee, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_quartiles,
    SIZE(COLLECT_SET(channel)) AS num_channels
FROM enriched_order_items
GROUP BY CASE WHEN next_unit_price >= 430 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY next_unit_price_tier DESC NULLS LAST
