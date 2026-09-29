select id, unit_price from order_items where unit_price <= (select min(unit_price) from order_items);
