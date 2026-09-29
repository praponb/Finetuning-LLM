select tag, sum(stock_qty) as total_stock from products lateral view explode(tags) t as tag where launch_date >= '2025-01-01' group by tag order by total_stock desc, tag;
