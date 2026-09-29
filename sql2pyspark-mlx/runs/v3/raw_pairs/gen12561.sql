select tags, supplier_id, array_join(tags, '|') as tags_text, date_trunc('WEEK', launch_date) as launch_date_period from products where price <= 840 or brand = 'Globex' order by supplier_id desc;
