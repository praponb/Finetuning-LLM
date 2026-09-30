select amount from payments union select unit_price from order_items where order_id in (5, 18, 19, 31) order by amount
