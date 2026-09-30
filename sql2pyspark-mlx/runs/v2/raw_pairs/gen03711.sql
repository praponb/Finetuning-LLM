select product_name, category, stock_qty, initcap(trim(product_name)) as product_name_title from products where array_contains(tags, 'clearance') order by stock_qty;
