select order_id, count(*) as cnt, min(unit_price) as min_unit_price, approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles from order_items group by order_id
