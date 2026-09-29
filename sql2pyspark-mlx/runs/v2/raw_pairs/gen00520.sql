select s.country, min(p.launch_date) as first_launch_date from products p inner join suppliers s on p.supplier_id = s.id group by s.country order by s.country
