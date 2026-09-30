select
    id,
    category,
    cost,
    launch_date
from (
    select *, rank() over (partition by category order by cost) rn
    from products
) r
where rn <= 3
