select count(quantity) as quantity_count from order_items where unit_price > 210 order by quantity_count desc;
