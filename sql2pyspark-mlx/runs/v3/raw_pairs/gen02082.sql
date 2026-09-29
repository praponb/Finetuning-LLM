SELECT
    suppliers.country,
    suppliers.rating,
    suppliers.supplier_name,
    products.launch_date
FROM suppliers
LEFT JOIN products ON products.supplier_id = suppliers.id
