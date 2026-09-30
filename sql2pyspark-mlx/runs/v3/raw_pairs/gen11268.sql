SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS pos FROM store_sales) AS ranked WHERE pos <= 5 ORDER BY 6 DESC;
