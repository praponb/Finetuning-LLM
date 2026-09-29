select *
from (
    select *, rank() over (partition by order_id order by quantity) as rnk
    from order_items
    where order_id in (8, 18)
) as t
where rnk = 1
order by rnk desc;
