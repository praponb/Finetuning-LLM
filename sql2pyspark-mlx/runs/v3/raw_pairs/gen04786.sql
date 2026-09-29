select s.id, s.region, s.store_name from stores s where s.city = 'Chiang Mai' and s.id in (select store_id from store_sales where store_id in (1, 2, 3, 5)) order by store_name desc
