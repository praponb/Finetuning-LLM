select id, stock_qty, product_name, concat(brand, ' ', category) as brand_category, cast(price as int) as price_int from products where brand <> 'Initech';
