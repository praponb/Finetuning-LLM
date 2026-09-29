select p.category, p.launch_date, p.price, s.city, s.supplier_name from products p left join suppliers s on p.supplier_id = s.id where p.cost between 530 and 720 and p.cost <= 540
