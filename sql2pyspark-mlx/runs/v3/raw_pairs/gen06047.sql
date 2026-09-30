select p.id, p.stock_qty from products p where exists (select 1 from inventory i where i.product_id = p.id)
