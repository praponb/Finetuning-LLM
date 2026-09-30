SELECT ss.units, ss.sale_date, ss.revenue, p.price FROM store_sales ss JOIN products p ON ss.product_id = p.id
