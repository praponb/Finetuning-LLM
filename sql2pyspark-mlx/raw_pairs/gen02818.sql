SELECT products.launch_date, products.product_name, suppliers.rating FROM products LEFT OUTER JOIN suppliers ON products.supplier_id = suppliers.id WHERE products.price > 330
