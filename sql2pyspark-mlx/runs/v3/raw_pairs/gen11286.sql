select id, category, supplier_id, launch_date, dense_rank() over (partition by category, supplier_id order by launch_date desc) as launch_date_dense_rank from products;
