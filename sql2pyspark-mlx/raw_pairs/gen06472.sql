WITH cleaned AS (
    SELECT
        *,
        ROUND(unit_price * 1.15, 2) adjusted_unit_price,
        GREATEST(quantity, unit_price) greatest_quantity_unit_price
    FROM order_items
    WHERE product_id IS NULL
)
SELECT
    p.status,
    COUNT(*) AS cnt,
    MAX(a.quantity) AS max_quantity,
    GROUPING_ID() AS grouping_level
FROM cleaned a
LEFT JOIN orders p ON a.order_id = p.id
GROUP BY ROLLUP (p.status);
