select order_id, unit_price from order_items where not (unit_price between 120 and 140 and unit_price between 340 and 380 and unit_price <= 390);
