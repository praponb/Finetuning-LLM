select s.id, s.sqft from stores s where s.id in (select store_id from store_sales where product_id in (4, 5, 9))
