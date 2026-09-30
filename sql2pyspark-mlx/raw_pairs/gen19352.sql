select *
from (
    select *, rank() over (partition by store_id order by last_restock desc) as rnk
    from inventory
    where product_id = 5
) as r
where rnk <= 1
