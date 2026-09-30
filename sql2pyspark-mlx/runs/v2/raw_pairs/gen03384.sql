select id, product_name, brand, tag_name from products lateral view explode(tags) t as tag_name where brand in ('Globex', 'Umbrella', 'Stark');
