SELECT id, category FROM products WHERE category = 'Electronics' INTERSECT SELECT id, category FROM products WHERE category IN ('Fashion', 'Grocery');
