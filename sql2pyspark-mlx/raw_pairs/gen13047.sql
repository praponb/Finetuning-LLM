with ranked as (select id, supplier_id, stock_qty, launch_date, row_number() over (partition by supplier_id order by stock_qty desc, id) as rnk from products) select * from ranked where rnk <= 3
