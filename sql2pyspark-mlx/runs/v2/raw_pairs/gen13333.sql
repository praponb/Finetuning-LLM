select p.id, p.brand, p.stock_qty from products p where p.supplier_id not in (select id from suppliers where country <> 'US')
