select id, product_name, tag_name from products lateral view explode(tags) t as tag_name where brand in ('Acme', 'Stark')
