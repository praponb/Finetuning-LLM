WITH metrics AS (
    SELECT
        *,
        MIN(quantity) OVER (PARTITION BY store_id) AS store_id_min_quantity,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY quantity DESC NULLS FIRST) AS quantity_cume_dist
    FROM inventory
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.last_restock,
    s.quantity_cume_dist,
    (SELECT MIN(t2.amount) FROM orders t2 WHERE t2.store_id = s.store_id) AS min_order_amount
FROM metrics s
ORDER BY min_order_amount DESC NULLS LAST;
