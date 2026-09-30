SELECT id, tags, price, CASE WHEN category IN ('Home', 'Fashion') THEN 'Core' ELSE 'Other' END AS category_group FROM products;
