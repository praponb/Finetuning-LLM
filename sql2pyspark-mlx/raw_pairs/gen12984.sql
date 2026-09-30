with numbered as (
    select *, dense_rank() over (partition by order_id order by quantity desc) as rn
    from order_items
)
select *
from numbered
where rn <= 2;
