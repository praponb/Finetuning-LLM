WITH Numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date DESC, id) AS pos FROM stores) SELECT id, region, opened_date FROM Numbered WHERE pos <= 3;
