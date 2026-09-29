select p.price, p.brand, p.launch_date, ss.revenue from products p inner join store_sales ss on p.id = ss.product_id where ss.revenue >= 2400 order by revenue desc, launch_date;
