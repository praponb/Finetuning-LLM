select
    case when unit_price >= 350 then 'high' when unit_price >= 160 then 'mid' else 'low' end as unit_price_bucket,
    case when quantity >= 7 then 'high' when quantity >= 4 then 'mid' else 'low' end as quantity_bucket,
    count(distinct quantity) as unique_products,
    max(unit_price) as max_unit_price,
    round(avg(unit_price), 2) as avg_unit_price
from order_items
group by cube(case when unit_price >= 350 then 'high' when unit_price >= 160 then 'mid' else 'low' end, case when quantity >= 7 then 'high' when quantity >= 4 then 'mid' else 'low' end)
order by max_unit_price desc, unit_price_bucket;
