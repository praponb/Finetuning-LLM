with src as (
    select *, case when quantity >= 267 then 'High' when quantity >= 99 then 'Medium' else 'Low' end as quantity_band
    from inventory
    where store_id in (2, 4, 5, 6) and last_restock < '2024-03-01' and quantity = 108
),
summary as (
    select product_id, count(quantity) as quantity_count, min(quantity) as min_quantity
    from src
    group by product_id
)
select product_id, quantity_count, min_quantity
from summary
order by min_quantity desc
