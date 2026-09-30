select o.shipping_fee, t.max_unit_price from orders o left join (select order_id, max(unit_price) max_unit_price from order_items group by order_id) t on o.id = t.order_id order by max_unit_price;
