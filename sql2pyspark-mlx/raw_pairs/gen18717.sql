SELECT id, brand, metric_name, reading FROM (SELECT id, brand, cost, price FROM products) src UNPIVOT INCLUDE NULLS (reading FOR metric_name IN (cost, price))
