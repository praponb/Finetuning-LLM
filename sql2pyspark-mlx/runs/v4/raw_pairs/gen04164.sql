select id, price, category, tag from products lateral view explode(tags) t as tag where price between 460 and 1030
