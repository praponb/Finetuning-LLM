select id, product_name, brand, label from products lateral view explode(tags) t as label where category not in ('Books', 'Grocery')
