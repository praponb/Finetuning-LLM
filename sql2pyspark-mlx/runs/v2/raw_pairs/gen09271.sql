SELECT id, price, ROW_NUMBER() OVER (ORDER BY price DESC, id) AS rn FROM products WHERE category IN ('Books', 'Grocery');
