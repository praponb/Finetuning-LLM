select ss.store_id, ss.sale_date, p.product_name, p.stock_qty, s.city from store_sales ss left outer join products p on p.id = ss.product_id left outer join stores s on ss.store_id = s.id
