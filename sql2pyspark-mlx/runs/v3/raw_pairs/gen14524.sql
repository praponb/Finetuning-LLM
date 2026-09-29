select id, quantity, floor(quantity) as quantity_whole from order_items where quantity != 6 or unit_price > 370 order by quantity desc
