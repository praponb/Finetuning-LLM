select id, category, tag_name from products lateral view explode(tags) t as tag_name
