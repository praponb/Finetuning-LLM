SELECT
    products.id,
    products.launch_date,
    products.stock_qty,
    products.product_name
FROM products
WHERE products.supplier_id IN (SELECT id FROM suppliers WHERE country IN ('UK', 'TH'));
