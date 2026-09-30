with base as (
    select *
    from products
    where stock_qty > 242 and (supplier_id is null or size(tags) > 1)
),
summary as (
    select category, max(launch_date) as last_launch_date
    from base
    group by category
)
select category, last_launch_date
from summary
