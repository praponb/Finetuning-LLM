select
    ss.id,
    ss.revenue,
    ss.store_id,
    p.brand,
    p.stock_qty,
    date_format(p.launch_date, 'yyyy-MM-dd') as launch_date_label
from store_sales ss
left join products p on ss.product_id = p.id
where p.cost between 380 and 640 or p.cost is not null;
