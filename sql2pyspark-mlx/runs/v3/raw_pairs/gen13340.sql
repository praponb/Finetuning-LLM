with store_sales_by_store as (
    select store_id, round(stddev(revenue), 2) as stddev_revenue
    from store_sales
    group by store_id
)
select s.id, s.region, ssbs.stddev_revenue
from stores s
left join store_sales_by_store ssbs on s.id = ssbs.store_id
where ssbs.stddev_revenue >= 39
