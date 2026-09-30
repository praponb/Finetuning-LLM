select product_id from order_items where unit_price is not null union all select product_id from order_items where order_id in (21, 26) order by product_id
