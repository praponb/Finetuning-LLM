select id, brand, tag_name from products lateral view explode(tags) t as tag_name where price > 1040;
