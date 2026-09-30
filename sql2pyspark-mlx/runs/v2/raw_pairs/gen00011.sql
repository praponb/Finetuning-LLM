select id, unit_price from order_items where unit_price > (select avg(unit_price) from order_items where unit_price between 280 and 390);
