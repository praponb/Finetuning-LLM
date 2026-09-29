select id, product_name, tag from products lateral view explode(tags) t as tag
