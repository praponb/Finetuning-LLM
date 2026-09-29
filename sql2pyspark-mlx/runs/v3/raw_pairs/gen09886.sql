with filtered_orders as (
    select *
    from orders
    where store_id is not null
),
summary as (
    select case when discount >= 0.31 then 'high' when discount >= 0.17 then 'mid' else 'low' end as discount_bucket, round(stddev(discount), 2) as stddev_discount
    from filtered_orders
    group by case when discount >= 0.31 then 'high' when discount >= 0.17 then 'mid' else 'low' end
)
select discount_bucket, stddev_discount
from summary
order by stddev_discount desc;
