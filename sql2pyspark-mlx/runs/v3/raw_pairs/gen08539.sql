SELECT
    id,
    product_id,
    last_restock,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) row_num
FROM inventory
WHERE MONTH(last_restock) IN (2, 10, 11);
