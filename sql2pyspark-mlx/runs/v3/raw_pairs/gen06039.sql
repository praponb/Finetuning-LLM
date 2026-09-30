SELECT
    suppliers.city,
    products.stock_qty,
    products.price,
    products.product_name,
    MONTH(products.launch_date) AS launch_date_month
FROM suppliers
LEFT JOIN products ON products.supplier_id = suppliers.id
WHERE products.stock_qty >= 113 AND products.category IN ('Books', 'Fashion', 'Home')
ORDER BY price ASC
