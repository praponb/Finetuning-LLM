select store_id, product_id, count(*) as row_count, corr(units, revenue) as corr_units_revenue, corr(revenue, units) as corr_revenue_units from store_sales group by 1, 2;
