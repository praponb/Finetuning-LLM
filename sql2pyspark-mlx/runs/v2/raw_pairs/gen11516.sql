select quarter(sale_date) as sale_quarter, count(*) as cnt from store_sales where sale_date between '2025-05-01' and '2025-07-01' group by quarter(sale_date) order by sale_quarter desc;
