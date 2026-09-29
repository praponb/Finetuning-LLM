select 'order_item' origin, unit_price from order_items where unit_price is not null union all select 'payment' origin, amount from payments where amount is not null
