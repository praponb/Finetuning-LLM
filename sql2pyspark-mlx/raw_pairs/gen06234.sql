select
    id,
    cost,
    stock_qty,
    sort_array(tags) as sorted_tags,
    add_months(launch_date, 6) as launch_date_plus_months
from products
where brand not in ('Umbrella', 'Acme') and brand = 'Initech'
order by stock_qty, id desc
limit 20;
