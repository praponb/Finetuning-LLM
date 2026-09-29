with filtered_store_sales as (
    select *
    from store_sales
    where month(sale_date) in (1, 7, 8)
),
summary as (
    select product_id, sum(revenue) as total_revenue, max(units) as max_units
    from filtered_store_sales
    group by product_id
)
select *
from summary
order by product_id desc, max_units;
