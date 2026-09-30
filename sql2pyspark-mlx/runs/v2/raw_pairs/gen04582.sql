select id, order_id, ceil(quantity) as quantity_whole, round(unit_price, 2) as unit_price_rounded from order_items;
