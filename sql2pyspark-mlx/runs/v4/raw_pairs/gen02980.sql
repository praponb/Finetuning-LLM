select s.* from suppliers s where exists (select 1 from products c where c.supplier_id = s.id and c.stock_qty between 294 and 326)
