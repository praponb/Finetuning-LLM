select order_id, count(unit_price) as unit_price_count, count(quantity) as quantity_count, max(unit_price) as max_unit_price from order_items group by order_id
