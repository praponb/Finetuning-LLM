SELECT * FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY units DESC) AS pos FROM store_sales) AS t WHERE pos = 1
