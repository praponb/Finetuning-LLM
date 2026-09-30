select
    case when unit_price >= 400 then 'high' when unit_price >= 170 then 'mid' else 'low' end as unit_price_bucket,
    round(avg(quantity), 2) as avg_quantity,
    count(distinct unit_price) as unique_products,
    sum(quantity) as total_quantity
from order_items
where quantity between 6 and 8
group by 1
order by 4 desc
