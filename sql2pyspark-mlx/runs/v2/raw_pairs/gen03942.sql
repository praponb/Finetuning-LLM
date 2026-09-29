SELECT tags, price FROM products WHERE (launch_date > '2023-06-24' AND supplier_id IN (8, 25, 39)) OR brand IN ('Stark', 'Globex');
