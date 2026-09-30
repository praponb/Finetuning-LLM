select quarter(sale_date) as sale_quarter, count(distinct revenue) as unique_products from store_sales group by quarter(sale_date) order by sale_quarter asc;
