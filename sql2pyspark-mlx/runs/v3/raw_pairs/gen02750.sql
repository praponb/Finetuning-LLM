SELECT
    id,
    supplier_id,
    price,
    brand,
    stock_qty,
    MAX(price) OVER (PARTITION BY supplier_id) AS supplier_id_max_price,
    LAST_VALUE(price) OVER (PARTITION BY brand ORDER BY stock_qty DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_price
FROM products
WHERE ARRAY_CONTAINS(tags, 'eco')
ORDER BY id DESC, stock_qty;
