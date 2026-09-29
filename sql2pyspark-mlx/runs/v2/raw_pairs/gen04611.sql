select id, store_id, round(revenue * 1.1, 2) as revenue_adjusted, date_trunc('YEAR', sale_date) as sale_date_period from store_sales where units > 15;
