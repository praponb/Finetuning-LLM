select ss.id, ss.store_id, ss.revenue, s.opened_date, p.brand, p.stock_qty from store_sales ss left join stores s on ss.store_id = s.id inner join products p on ss.product_id = p.id
