select s.city, s.country, s.supplier_name, p.cost, p.price from suppliers s left join products p on p.supplier_id = s.id where array_contains(p.tags, 'bestseller') and p.product_name like '%Mat%';
