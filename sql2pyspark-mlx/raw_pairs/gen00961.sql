select id, sale_date, store_id, units, min(units) over (partition by date_trunc('week', sale_date), store_id) as sale_date_period_min_units from store_sales where units = 23 order by sale_date desc
