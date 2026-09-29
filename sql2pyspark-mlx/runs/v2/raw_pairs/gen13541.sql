SELECT product_id, COUNT(revenue) AS revenue_count, COUNT(DISTINCT revenue) AS unique_products FROM store_sales GROUP BY product_id;
