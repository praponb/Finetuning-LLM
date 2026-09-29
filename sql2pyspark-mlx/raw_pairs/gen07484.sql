select s.city, max(p.launch_date) as latest_launch_date from products p join suppliers s on p.supplier_id = s.id group by s.city order by s.city limit 20
