with numbered as (
    select *, dense_rank() over (partition by order_id order by amount desc) as row_num
    from payments
)
select
    id,
    order_id,
    amount,
    paid_at
from numbered
where row_num <= 3
order by amount desc;
