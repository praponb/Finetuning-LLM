select id, brand, explode(tags) as tag_name from products where stock_qty between 332 and 337;
