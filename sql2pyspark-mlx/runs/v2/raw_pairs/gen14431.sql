select s.country, max(p.price) - min(p.price) as price_range from products p join suppliers s on p.supplier_id = s.id group by s.country order by price_range, country desc
