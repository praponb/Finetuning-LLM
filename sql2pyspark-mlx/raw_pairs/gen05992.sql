select 'order_item' origin, unit_price from order_items where order_id in (13, 14, 21, 31) union all select 'transaction' origin, amount from transactions order by unit_price;
