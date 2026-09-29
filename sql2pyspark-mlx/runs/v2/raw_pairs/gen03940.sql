select ss.id, ss.product_id, ss.revenue, s.opened_date, p.category from store_sales ss left join stores s on s.id = ss.store_id inner join products p on ss.product_id = p.id;
