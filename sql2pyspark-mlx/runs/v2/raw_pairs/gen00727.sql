select s.supplier_name, s.city, p.stock_qty, p.brand, initcap(trim(p.category)) as category_title from suppliers s left outer join products p on p.supplier_id = s.id;
