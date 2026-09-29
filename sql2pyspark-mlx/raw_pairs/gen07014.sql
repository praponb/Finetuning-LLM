select id, brand, product_name, label from products lateral view explode(tags) t as label
