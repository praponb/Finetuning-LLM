SELECT suppliers.country, suppliers.supplier_name, products.product_name, products.brand, products.stock_qty FROM suppliers LEFT JOIN products ON suppliers.id = products.supplier_id;
