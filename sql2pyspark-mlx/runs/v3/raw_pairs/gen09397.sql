select ss.product_id, ss.sale_date, s.sqft, s.opened_date, p.launch_date from store_sales ss join stores s on s.id = ss.store_id join products p on p.id = ss.product_id where ss.revenue <= 1200
