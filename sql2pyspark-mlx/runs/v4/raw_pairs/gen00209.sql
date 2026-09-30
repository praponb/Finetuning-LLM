WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY last_restock DESC) rnk
    FROM inventory
    WHERE product_id = 1
)
SELECT
    id,
    store_id,
    last_restock,
    product_id
FROM numbered
WHERE rnk <= 3;
