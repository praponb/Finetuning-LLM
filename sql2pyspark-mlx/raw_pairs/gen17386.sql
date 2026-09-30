SELECT id, launch_date, tags, price FROM products WHERE NOT (cost BETWEEN 210 AND 720 OR cost < 630);
