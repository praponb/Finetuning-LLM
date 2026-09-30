select label, sum(stock_qty) as total_stock from products lateral view explode(tags) t as label where stock_qty is not null group by label
