SELECT
    id,
    product_id,
    quantity,
    DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS dense_rnk
FROM inventory;
