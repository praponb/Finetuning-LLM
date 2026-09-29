select
    id,
    supplier_id,
    launch_date,
    stock_qty,
    lead(stock_qty, 2) over (partition by supplier_id order by launch_date, id) as next_stock_qty
from products;
