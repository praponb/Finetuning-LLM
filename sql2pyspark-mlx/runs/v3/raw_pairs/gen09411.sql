with ranked as (
    select
        id,
        store_id,
        discount,
        customer_id,
        row_number() over (partition by store_id order by discount desc, id) as rn
    from orders
)
select *
from ranked
where rn <= 3
order by 5 desc;
