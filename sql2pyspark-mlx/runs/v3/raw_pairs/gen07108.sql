select *, quantity * unit_price as line_total from order_items where unit_price between 180 and 270 order by 4 desc, id limit 20;
