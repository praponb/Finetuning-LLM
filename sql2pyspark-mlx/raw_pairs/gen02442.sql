select id, sale_date, rank() over (order by sale_date) as sale_date_rank from store_sales where year(sale_date) = 2024;
