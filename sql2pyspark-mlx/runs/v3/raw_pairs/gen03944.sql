SELECT id, region, opened_date FROM (SELECT id, region, opened_date, DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS pos FROM stores) AS r WHERE pos <= 3;
