with ranked_payments as (
    select *, rank() over (partition by order_id order by amount) as pos
    from payments
    where amount is not null
)
select *
from ranked_payments
where pos <= 2
order by 3
