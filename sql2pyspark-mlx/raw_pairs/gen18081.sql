select o.id, o.discount, o.store_id, o.order_date from orders o where o.id in (select order_id from order_items where unit_price between 280 and 340)
