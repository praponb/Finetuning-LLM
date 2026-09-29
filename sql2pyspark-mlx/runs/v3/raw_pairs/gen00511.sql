SELECT
    suppliers.id,
    suppliers.city,
    t.avg_price,
    t.max_stock_qty
FROM suppliers
INNER JOIN (SELECT supplier_id, AVG(price) AS avg_price, MAX(stock_qty) AS max_stock_qty
    FROM products
    GROUP BY supplier_id) t ON suppliers.id = t.supplier_id
ORDER BY avg_price DESC
