select p.id, p.cost, p.brand, p.category from products p where p.supplier_id in (select id from suppliers where city in ('Bangkok', 'Berlin')) order by id desc;
