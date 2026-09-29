select p.id, p.stock_qty from products p where p.supplier_id in (select id from suppliers where rating < 4.1)
