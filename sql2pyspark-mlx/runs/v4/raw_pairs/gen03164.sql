select label, sum(stock_qty) as total_stock from products lateral view explode(tags) t as label group by label;
