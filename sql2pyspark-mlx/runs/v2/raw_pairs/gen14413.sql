select id, supplier_id from products where supplier_id is not null intersect select id, supplier_id from products where stock_qty between 167 and 299
