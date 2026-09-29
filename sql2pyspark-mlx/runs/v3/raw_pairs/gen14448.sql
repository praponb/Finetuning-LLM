select id, order_id, round(unit_price * 0.9, 2) as unit_price_scaled from order_items where quantity >= 6 or quantity > 5;
