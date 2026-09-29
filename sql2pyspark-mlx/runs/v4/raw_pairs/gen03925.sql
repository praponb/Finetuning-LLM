select id, category from products where brand not in ('Acme', 'Globex') intersect select id, category from products where stock_qty < 158
