select product_id, unit_price, round(quantity, 1) quantity_rounded from order_items where unit_price > 370 or quantity between 4 and 6
