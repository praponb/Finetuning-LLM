select p.category, p.stock_qty, ss.revenue from products p join store_sales ss on p.id = ss.product_id where p.brand in ('Umbrella', 'Stark');
