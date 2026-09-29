select ss.id, ss.revenue, s.region, s.store_name, p.category from store_sales ss left join stores s on ss.store_id = s.id right join products p on ss.product_id = p.id
