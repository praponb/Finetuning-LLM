SELECT
    id,
    order_id,
    product_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity NULLS FIRST, id) AS latest_rank
    FROM order_items
) x
WHERE latest_rank = 1
