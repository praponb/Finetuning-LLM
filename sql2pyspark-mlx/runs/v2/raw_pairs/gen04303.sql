select id, brand, explode(tags) as tag_name from products where product_name like '%Shoe%';
