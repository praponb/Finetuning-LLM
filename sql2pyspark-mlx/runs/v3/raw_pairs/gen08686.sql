with filtered_store_sales as (
    select *
    from store_sales
    where units = 38 and store_id in (2, 4) and revenue is null
),
agg as (
    select store_id, max(sale_date) as last_sale_date, round(stddev(revenue), 2) as stddev_revenue
    from filtered_store_sales
    group by store_id
)
select store_id, last_sale_date, stddev_revenue
from agg
where stddev_revenue > 25
order by stddev_revenue desc
