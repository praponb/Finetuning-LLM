select p.category, p.launch_date, p.supplier_id, s.supplier_name, s.city from products p inner join suppliers s on p.supplier_id = s.id where s.rating > 2.4 and s.city = 'Bangkok'
