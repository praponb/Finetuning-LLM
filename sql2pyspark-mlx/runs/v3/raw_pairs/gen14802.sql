select id, unit_price from order_items where unit_price >= (select max(unit_price) from order_items);
