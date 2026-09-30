with numbered as (
    select
        id,
        order_id,
        quantity,
        rank() over (partition by order_id order by quantity desc) rn
    from order_items
)
select *
from numbered
where rn <= 3
order by id
