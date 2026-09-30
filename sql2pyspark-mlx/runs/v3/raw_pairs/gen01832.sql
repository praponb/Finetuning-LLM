select label, sum(stock_qty) as total_stock from products lateral view explode(tags) t as label where brand = 'Initech' group by label order by total_stock desc, label
