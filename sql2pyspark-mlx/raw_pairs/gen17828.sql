SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units, id) AS row_num FROM store_sales) AS ranked WHERE row_num <= 2;
