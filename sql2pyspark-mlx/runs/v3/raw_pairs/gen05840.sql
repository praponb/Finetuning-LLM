select id, price, product_name, tag from products lateral view explode(tags) t as tag where supplier_id in (2, 21, 34)
