SELECT * FROM (SELECT product_id, MAX(revenue) - MIN(revenue) AS revenue_range FROM store_sales GROUP BY product_id) AS x WHERE revenue_range >= 35 ORDER BY 1;
