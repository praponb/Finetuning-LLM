select id, brand, tag from products lateral view explode(tags) t as tag where upper(trim(product_name)) = 'NOISE-CANCEL HEADPHONES'
