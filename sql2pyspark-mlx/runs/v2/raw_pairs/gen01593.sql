select s.id, s.region, s.city, t.avg_units from stores s join (select store_id, avg(units) as avg_units from store_sales group by store_id) t on s.id = t.store_id;
