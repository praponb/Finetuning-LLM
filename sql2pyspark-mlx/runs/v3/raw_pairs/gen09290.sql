with src as (
    select *, cast(quantity as string) as quantity_str
    from inventory
    where last_restock between '2023-05-01' and '2025-04-01' and product_id in (3, 5, 10)
),
summary as (
    select case when quantity >= 203 then 'high' when quantity >= 42 then 'mid' else 'low' end as quantity_bucket, round(avg(quantity), 2) as avg_quantity
    from src
    group by case when quantity >= 203 then 'high' when quantity >= 42 then 'mid' else 'low' end
)
select quantity_bucket, avg_quantity
from summary
where avg_quantity >= 42
order by avg_quantity desc
