with src as (
    select *
    from products
    where not (price >= 770 or brand = 'Umbrella')
),
summary as (
    select case when stock_qty >= 410 then 'high' when stock_qty >= 139 then 'mid' else 'low' end as stock_qty_bucket, brand, round(avg(price), 2) as avg_price
    from src
    group by case when stock_qty >= 410 then 'high' when stock_qty >= 139 then 'mid' else 'low' end, brand
)
select *
from summary
where avg_price >= 73;
