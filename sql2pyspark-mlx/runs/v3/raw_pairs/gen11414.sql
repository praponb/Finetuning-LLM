SELECT id, price, CASE WHEN category IN ('Grocery', 'Home') THEN 'Priority' ELSE 'Other' END AS category_group FROM products WHERE price BETWEEN 540 AND 880 AND brand IN ('Globex', 'Stark');
