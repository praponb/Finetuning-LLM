select p.id, p.price, p.cost, p.launch_date from products p where p.supplier_id in (select id from suppliers where city not in ('Singapore', 'New York')) order by p.cost desc, p.price desc;
