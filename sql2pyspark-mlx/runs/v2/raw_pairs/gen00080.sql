select s.id, s.rating from suppliers s where exists (select 1 from products p where p.supplier_id = s.id and upper(trim(p.product_name)) = 'BACKPACK XL');
