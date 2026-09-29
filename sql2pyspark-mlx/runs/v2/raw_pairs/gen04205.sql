select id, brand, category, label from products lateral view explode(tags) t as label where supplier_id is not null;
