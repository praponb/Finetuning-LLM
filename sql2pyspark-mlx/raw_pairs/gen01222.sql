SELECT cost FROM products WHERE cost IS NOT NULL EXCEPT SELECT amount FROM payments WHERE amount IS NOT NULL ORDER BY cost
