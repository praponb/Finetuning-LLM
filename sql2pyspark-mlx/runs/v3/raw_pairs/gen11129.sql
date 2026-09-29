SELECT
    id,
    store_id,
    quantity,
    product_id
FROM (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY quantity) rn
    FROM inventory
    WHERE quantity < 218
) r
WHERE rn <= 3
ORDER BY store_id DESC, product_id DESC;
