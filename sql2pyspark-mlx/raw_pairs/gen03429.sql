select id, price, tag from products lateral view explode(tags) t as tag where brand <> 'Stark';
