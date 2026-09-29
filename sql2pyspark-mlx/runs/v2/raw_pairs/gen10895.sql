select a.id, a.order_id, a.unit_price from order_items a where a.unit_price = (select max(b.unit_price) from order_items b where b.order_id = a.order_id) order by a.order_id desc;
