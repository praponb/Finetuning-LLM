WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        AVG(quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior
    FROM inventory
    WHERE product_id IN (1, 3, 4, 8)
)
SELECT
    s.id,
    s.product_id,
    s.last_restock,
    s.last_known_quantity,
    s.quantity_avg_5_prior,
    (SELECT MAX(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS max_inventory_quantity
FROM metrics s
ORDER BY id DESC;
