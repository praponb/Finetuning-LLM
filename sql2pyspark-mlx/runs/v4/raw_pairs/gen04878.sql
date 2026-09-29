SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) AS rn FROM store_sales WHERE units > 35) AS t WHERE rn <= 2 ORDER BY rn DESC;
