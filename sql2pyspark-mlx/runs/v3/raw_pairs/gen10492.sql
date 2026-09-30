select id, supplier_id, category, launch_date, initcap(trim(product_name)) as product_name_title from products order by id desc;
