WITH deduped AS (
    SELECT *
    FROM order_items
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC NULLS FIRST, id) = 1
)
SELECT
    id,
    order_id,
    product_id,
    unit_price
FROM deduped;
