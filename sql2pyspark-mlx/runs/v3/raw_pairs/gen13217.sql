select id, order_id, unit_price from (select *, row_number() over (partition by order_id order by unit_price desc, id) as rn from order_items) as t where rn = 1;
