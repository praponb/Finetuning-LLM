select s.id, s.supplier_name, s.rating from suppliers s where s.id in (select supplier_id from products where brand not in ('Stark', 'Initech')) order by s.id desc, rating desc limit 20;
