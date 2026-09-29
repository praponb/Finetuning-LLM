select s.id, s.city, t.min_sale_date from stores s left join (select store_id, min(sale_date) as min_sale_date from store_sales group by store_id) t on s.id = t.store_id order by id asc, city desc
