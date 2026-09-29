with numbered as (
    select
        id,
        order_id,
        unit_price,
        row_number() over (partition by order_id order by unit_price desc, id) as pos
    from order_items
)
select *
from numbered
where pos <= 2
order by id desc;
