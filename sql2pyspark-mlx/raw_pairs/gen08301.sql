SELECT price FROM products WHERE ARRAY_CONTAINS(tags, 'new') EXCEPT SELECT cost FROM shipments ORDER BY price
