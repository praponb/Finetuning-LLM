SELECT * FROM (SELECT supplier_id, category, cost FROM products) src PIVOT (MAX(cost) AS value_sum, COUNT(1) AS item_count FOR category IN ('Home', 'Electronics'));
