select order_id from order_items where quantity > 4 union select order_id from order_items where quantity between 3 and 7 order by order_id
