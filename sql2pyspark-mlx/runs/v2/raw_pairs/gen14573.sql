select
    i.product_id,
    p.stock_qty,
    p.cost,
    p.launch_date
from inventory i
full outer join products p on i.product_id = p.id
where p.category <> 'Fashion' and p.brand in ('Acme', 'Stark', 'Initech')
