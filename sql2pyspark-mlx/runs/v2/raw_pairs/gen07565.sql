select ss.id, ss.revenue, p.brand, s.sqft, s.store_name from store_sales ss left outer join products p on ss.product_id = p.id inner join stores s on ss.store_id = s.id
