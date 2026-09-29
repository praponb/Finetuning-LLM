select
    id,
    brand,
    launch_date,
    supplier_id,
    stock_qty,
    ntile(2) over (partition by brand order by launch_date desc, id) as launch_date_ntile,
    lag(stock_qty, 1) over (partition by supplier_id order by launch_date, id) as prev_stock_qty
from products
where brand = 'Acme'
order by 2
