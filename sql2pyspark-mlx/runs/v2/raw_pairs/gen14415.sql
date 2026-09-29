SELECT
    id,
    product_name,
    supplier_id,
    brand
FROM products
WHERE stock_qty BETWEEN 254 AND 274 AND category IN ('Fashion', 'Home', 'Grocery')
