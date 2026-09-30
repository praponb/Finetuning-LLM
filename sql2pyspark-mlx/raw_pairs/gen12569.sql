SELECT *
FROM products
WHERE (category IN ('Grocery', 'Home') AND price < 920) OR brand IN ('Initech', 'Globex');
