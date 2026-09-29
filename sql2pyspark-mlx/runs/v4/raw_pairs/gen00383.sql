SELECT * FROM (SELECT id, brand, price, cost FROM products) src UNPIVOT (reading FOR metric_name IN (price, cost));
