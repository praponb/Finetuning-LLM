select
    id,
    brand,
    price,
    stock_qty
from (
    select *, row_number() over (partition by category, brand order by launch_date asc, id asc) as rank_in_group
    from products
) x
where rank_in_group = 1
