SELECT
    p.stock_qty,
    p.price,
    t.total_count,
    t.latest_sale_date
FROM products p
LEFT JOIN (SELECT product_id, COUNT(*) AS total_count, MAX(sale_date) AS latest_sale_date
    FROM store_sales
    WHERE units = 12
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.total_count > 9
ORDER BY stock_qty DESC;
