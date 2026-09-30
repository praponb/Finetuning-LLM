select s.rating, s.supplier_name, p.product_name, p.cost from suppliers s left join products p on p.supplier_id = s.id order by p.product_name;
