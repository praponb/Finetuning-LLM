with top_order_items as (select *, row_number() over (partition by order_id order by unit_price, id) pos from order_items) select id, order_id, unit_price from top_order_items where pos <= 1;
