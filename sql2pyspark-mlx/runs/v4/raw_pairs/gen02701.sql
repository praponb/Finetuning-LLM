SELECT amount FROM orders WHERE status <> 'RETURNED' INTERSECT SELECT price FROM products WHERE supplier_id IS NOT NULL ORDER BY amount;
