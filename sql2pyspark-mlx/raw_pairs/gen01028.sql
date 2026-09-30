select id, launch_date, tags from products where not (supplier_id = 24 and supplier_id = 37) order by id desc;
