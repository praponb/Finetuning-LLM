select id, quantity, order_id from order_items where quantity >= 3 union select id, quantity, order_id from order_items where unit_price between 250 and 280;
