select id, unit_price, round(unit_price, 2) as unit_price_rounded, round(quantity / unit_price, 2) as line_total from order_items where quantity >= 7 order by 3 desc, id limit 10
