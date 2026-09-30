select s.region, t.unique_products from stores s join (select store_id, count(distinct revenue) as unique_products from store_sales group by store_id) t on s.id = t.store_id order by s.region
