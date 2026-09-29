select tag, sum(stock_qty) as total_stock from products lateral view explode(tags) t as tag where cost between 330 and 490 group by tag order by total_stock desc, tag
