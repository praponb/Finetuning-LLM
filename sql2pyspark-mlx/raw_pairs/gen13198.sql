with numbered as (
    select
        id,
        order_id,
        quantity,
        product_id,
        row_number() over (partition by order_id order by quantity desc, id) as rnk
    from order_items
)
select
    id,
    order_id,
    quantity,
    product_id
from numbered
where rnk <= 3
order by id desc
