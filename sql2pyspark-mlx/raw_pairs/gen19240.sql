select quarter(sale_date) sale_quarter, product_id, count(revenue) revenue_count, count(units) units_count from store_sales group by quarter(sale_date), product_id having units_count >= 1;
