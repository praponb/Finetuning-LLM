SELECT method FROM payments WHERE method IS NOT NULL EXCEPT SELECT brand FROM products WHERE brand IS NOT NULL ORDER BY method
