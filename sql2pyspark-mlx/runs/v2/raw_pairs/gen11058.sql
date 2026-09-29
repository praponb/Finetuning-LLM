select id, order_id, unit_price, avg(unit_price) over (partition by order_id) as order_id_avg_unit_price from order_items where quantity between 5 and 6 order by order_id desc
