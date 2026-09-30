select order_id, count(*) as total_count, count(*) as order_item_count from order_items where unit_price > 140 group by order_id having count(*) >= 4 order by order_item_count asc, total_count asc;
