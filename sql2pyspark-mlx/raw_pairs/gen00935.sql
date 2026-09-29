select tag_name, sum(stock_qty) as total_stock from products lateral view explode(tags) t as tag_name where brand = 'Umbrella' group by tag_name
