select id, price, explode(tags) as tag_name from products;
