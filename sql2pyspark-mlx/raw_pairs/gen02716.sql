WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        product_id
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS rn
        FROM order_items
    ) ranked
    WHERE rn = 1
)
-- Grouped metrics
SELECT
    order_id,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    COUNT(*) AS num_rows_2
FROM deduped
GROUP BY order_id
ORDER BY order_id DESC NULLS LAST
