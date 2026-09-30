WITH base AS (
    SELECT *
    FROM order_items
    WHERE quantity BETWEEN 3 AND 4 AND (product_id IS NULL OR quantity BETWEEN 3 AND 5)
),
ProductIdSummary AS (
    SELECT
        product_id,
        CASE WHEN unit_price >= 300 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
        MIN(unit_price) AS min_unit_price,
        COUNT(*) AS num_order_items
    FROM base
    GROUP BY product_id, CASE WHEN unit_price >= 300 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END
)
SELECT
    product_id,
    unit_price_bucket,
    min_unit_price,
    num_order_items
FROM ProductIdSummary
ORDER BY num_order_items, 1 DESC, unit_price_bucket
LIMIT 10;
