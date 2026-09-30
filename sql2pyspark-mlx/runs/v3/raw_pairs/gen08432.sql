SELECT suppliers.supplier_name, suppliers.rating, suppliers.city, products.brand, products.product_name, products.stock_qty FROM suppliers LEFT JOIN products ON products.supplier_id = suppliers.id
