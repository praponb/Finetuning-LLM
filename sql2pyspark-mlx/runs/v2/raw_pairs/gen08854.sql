select * from (select order_id, max(quantity) - min(quantity) as quantity_range from order_items group by order_id) as s where quantity_range >= 49 order by 1;
