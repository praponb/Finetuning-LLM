select ss.id, ss.revenue, p.price, p.cost, s.store_name from store_sales ss inner join products p on p.id = ss.product_id inner join stores s on ss.store_id = s.id
