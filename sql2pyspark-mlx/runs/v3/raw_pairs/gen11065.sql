select id, product_id, least(quantity, unit_price) as least_quantity_unit_price from order_items where unit_price > 200 order by id;
