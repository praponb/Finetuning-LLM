select id, store_id, units, revenue, least(units, revenue) as least_units_revenue from store_sales where store_id in (2, 6)
