select p.category, ss.revenue, ss.sale_date, ss.store_id from products p left join store_sales ss on ss.product_id = p.id
