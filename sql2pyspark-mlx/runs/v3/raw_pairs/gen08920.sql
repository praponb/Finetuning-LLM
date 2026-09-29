with ProductSummary as (
    select product_id, round(avg(units), 2) as avg_units, round(avg(revenue), 2) as avg_revenue
    from store_sales
    where store_id in (1, 2, 4, 5)
    group by product_id
)
select
    p.cost,
    p.product_name,
    P.avg_units,
    P.avg_revenue
from products p
left join ProductSummary P on p.id = P.product_id
order by product_name, avg_units desc;
