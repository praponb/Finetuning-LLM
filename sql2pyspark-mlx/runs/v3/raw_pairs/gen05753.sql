select id, category, tag from products lateral view explode(tags) t as tag;
