select id, stock_qty, category from products where brand in ('Acme', 'Umbrella') union select id, stock_qty, category from products where array_contains(tags, 'sale');
