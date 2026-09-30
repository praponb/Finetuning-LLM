select ss.product_id, s.opened_date from store_sales ss left join stores s on ss.store_id = s.id where month(s.opened_date) in (3, 9, 10) order by product_id asc
