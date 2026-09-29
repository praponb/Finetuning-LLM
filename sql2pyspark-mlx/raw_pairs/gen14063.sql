select s.city, s.country, count(*) row_count from products p join suppliers s on p.supplier_id = s.id group by s.city, s.country;
