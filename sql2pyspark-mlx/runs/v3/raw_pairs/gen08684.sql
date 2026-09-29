select id, brand, price, tag from products lateral view explode(tags) t as tag;
