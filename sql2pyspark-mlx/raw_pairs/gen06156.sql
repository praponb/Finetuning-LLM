select id, price, brand, tag from products lateral view explode(tags) t as tag;
