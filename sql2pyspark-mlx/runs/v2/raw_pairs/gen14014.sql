SELECT id, supplier_id, product_name, cost, DATE_FORMAT(launch_date, 'MMM yyyy') AS launch_date_label, ARRAY_JOIN(tags, ', ') AS tags_text FROM products WHERE category <> 'Grocery';
