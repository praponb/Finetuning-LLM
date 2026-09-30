select s.id, s.region from stores s where s.id in (select store_id from store_sales)
