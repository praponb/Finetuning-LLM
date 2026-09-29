select p.id, p.stock_qty, p.launch_date, s.city from products p inner join suppliers s on s.id = p.supplier_id where p.brand = 'Initech' and s.supplier_name is not null order by s.city, id limit 50
