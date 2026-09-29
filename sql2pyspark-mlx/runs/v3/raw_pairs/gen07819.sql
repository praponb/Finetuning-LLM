select o.discount, t.quantity_count from orders o inner join (select order_id, count(quantity) quantity_count from order_items group by order_id) t on o.id = t.order_id
