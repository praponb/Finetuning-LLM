with valid_store_sales as (
    select *
    from store_sales
    where (sale_date > '2023-08-01' and sale_date <= '2023-10-27') or sale_date >= '2025-04-01'
),
agg as (
    select store_id, max(revenue) as max_revenue, round(stddev(revenue), 2) as stddev_revenue
    from valid_store_sales
    group by store_id
)
select store_id, max_revenue, stddev_revenue
from agg
where max_revenue >= 2300
order by store_id;
