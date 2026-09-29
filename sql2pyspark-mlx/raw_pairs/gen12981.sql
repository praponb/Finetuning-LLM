SELECT id, product_name FROM products WHERE brand NOT IN ('Umbrella', 'Acme', 'Stark') INTERSECT SELECT id, product_name FROM products WHERE SIZE(tags) >= 1;
