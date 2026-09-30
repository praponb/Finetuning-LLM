SELECT * FROM (SELECT id, product_id, revenue, units, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) AS rn FROM store_sales) AS ranked WHERE rn <= 1 ORDER BY units DESC;
