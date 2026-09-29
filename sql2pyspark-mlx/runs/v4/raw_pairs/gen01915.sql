WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        elem.product_id,
        elem.qty,
        elem.promo
    FROM (
        SELECT
            id,
            customer_id,
            EXPLODE(items) AS elem
        FROM carts
    ) li
)
SELECT
    id,
    customer_id,
    product_id,
    qty,
    promo
FROM flattened
ORDER BY qty NULLS FIRST;
