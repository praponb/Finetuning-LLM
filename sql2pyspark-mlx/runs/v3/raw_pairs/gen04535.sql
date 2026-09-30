SELECT DATE_FORMAT(sale_date, 'yyyy-MM') sale_month, COUNT(revenue) revenue_count, COUNT(DISTINCT revenue) unique_products FROM store_sales WHERE store_id = 3 GROUP BY sale_month
