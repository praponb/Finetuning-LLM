SELECT id, product_id, sale_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC NULLS LAST, id ASC) AS rn FROM store_sales) t WHERE rn = 1;
