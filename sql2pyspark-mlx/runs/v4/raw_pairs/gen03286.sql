SELECT ss.sale_date, p.stock_qty, s.rating
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
INNER JOIN suppliers s ON p.supplier_id = s.id
