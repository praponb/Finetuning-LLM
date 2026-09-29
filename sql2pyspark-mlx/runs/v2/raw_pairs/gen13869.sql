select tag, sum(stock_qty) as total_stock from products lateral view explode(tags) t as tag where stock_qty != 107 group by tag;
