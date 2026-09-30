SELECT suppliers.id, suppliers.supplier_name, COUNT(products.id) AS product_count
FROM suppliers
LEFT JOIN products ON products.supplier_id = suppliers.id
GROUP BY suppliers.id, suppliers.supplier_name;
