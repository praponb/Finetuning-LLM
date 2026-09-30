select tag_name, sum(stock_qty) as total_stock from products lateral view explode(tags) t as tag_name where month(launch_date) in (5, 9, 12) group by tag_name
