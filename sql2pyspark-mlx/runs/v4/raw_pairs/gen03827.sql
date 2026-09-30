select p.id, p.brand, p.stock_qty, p.category from products p where p.stock_qty = 289 and p.supplier_id not in (select id from suppliers where city not in ('Bangkok', 'Chiang Mai', 'New York'))
