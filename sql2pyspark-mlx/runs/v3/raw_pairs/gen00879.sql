select
    store_id,
    month(last_restock) as last_restock_month,
    case when quantity >= 265 then 'high' when quantity >= 46 then 'mid' else 'low' end as quantity_bucket,
    sum(quantity) as total_quantity,
    max(quantity) - min(quantity) as quantity_range
from inventory
where product_id in (1, 3, 5, 10)
group by 1, 2, 3
order by total_quantity desc
