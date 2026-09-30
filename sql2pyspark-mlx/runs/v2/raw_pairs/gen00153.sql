SELECT products.cost, suppliers.city, suppliers.rating, suppliers.supplier_name FROM products FULL OUTER JOIN suppliers ON products.supplier_id = suppliers.id;
