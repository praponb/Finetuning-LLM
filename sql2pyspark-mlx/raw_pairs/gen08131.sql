with numbered as (
    select
        id,
        store_id,
        discount,
        dense_rank() over (partition by store_id order by discount desc) as row_num
    from orders
)
select *
from numbered
where row_num = 1
