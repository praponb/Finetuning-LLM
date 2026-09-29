with filtered_orders as (
    select *, round(discount, 2) as discount_rounded
    from orders
    where store_id is null and store_id = 19
),
status_summary as (
    select status, sum(discount_rounded) as total_discount_rounded
    from filtered_orders
    group by status
)
select status, total_discount_rounded
from status_summary
where total_discount_rounded < 180
order by status desc
