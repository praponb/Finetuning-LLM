select order_id, max(quantity) - min(quantity) as quantity_range from order_items group by order_id having max(quantity) - min(quantity) >= 51 order by order_id desc limit 1;
