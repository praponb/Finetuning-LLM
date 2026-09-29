SELECT * FROM (SELECT brand, category, price FROM products) src PIVOT (AVG(price) AS value_sum, COUNT(1) AS item_count FOR category IN ('Electronics', 'Grocery', 'Home'));
