with latest as (select * from order_items qualify row_number() over (partition by product_id order by quantity desc, id) = 1) select id, order_id, quantity from latest;
