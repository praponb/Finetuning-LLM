with base as (
    select *, date_format(last_restock, 'yyyy-MM') as last_restock_label
    from inventory
    where last_restock >= to_date('2023-04-03')
      and store_id = 3
)
select
    id,
    product_id,
    quantity,
    last_restock_label
from base
qualify row_number() over (partition by store_id order by quantity desc, id) = 1
