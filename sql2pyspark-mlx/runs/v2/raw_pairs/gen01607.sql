SELECT * FROM (SELECT id, region, opened_date, RANK() OVER (PARTITION BY region ORDER BY opened_date) AS pos FROM stores) AS t WHERE pos <= 3;
