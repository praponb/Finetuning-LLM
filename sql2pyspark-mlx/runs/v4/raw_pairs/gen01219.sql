SELECT
    *,
    NTILE(5) OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS quantity_bucket,
    FIRST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity
FROM inventory;
