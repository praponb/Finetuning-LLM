select store_id, round(avg(units), 2) as avg_units, max(sale_date) as last_sale_date from store_sales where units = 34 group by store_id
