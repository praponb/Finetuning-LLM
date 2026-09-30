select id, store_id, sale_date, revenue, first_value(revenue) over (partition by store_id order by sale_date, id) as first_revenue from store_sales where units <= 29
