SELECT COUNT(*) AS row_count, MIN(sale_date) AS min_sale_date, COUNT(DISTINCT revenue) AS unique_stores FROM store_sales WHERE revenue <= 2100;
