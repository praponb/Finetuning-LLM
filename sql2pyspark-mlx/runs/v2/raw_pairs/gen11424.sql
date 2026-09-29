SELECT products.id, products.stock_qty
FROM products
WHERE products.supplier_id IN (SELECT id FROM suppliers WHERE country IN ('UK', 'DE', 'SG'))
