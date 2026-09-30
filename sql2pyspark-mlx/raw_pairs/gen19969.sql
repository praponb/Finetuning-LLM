with base as (
    select
        *,
        case when amount >= 1780 then 'High' when amount >= 410 then 'Medium' else 'Low' end as amount_level,
        coalesce(method, 'N/A') as method_filled,
        coalesce(method, 'DIRECT') as method_filled_2
    from payments
    where method is distinct from 'card'
),
joined as (
    select
        a.id,
        a.order_id,
        a.amount,
        a.amount_level,
        a.method_filled,
        a.method_filled_2,
        p.status as order_status,
        p.order_date,
        p.shipping_fee
    from base a
    join orders p
        on a.order_id = p.id
),
deduped as (
    select
        id,
        amount,
        amount_level,
        method_filled,
        order_status,
        order_date
    from joined
    qualify row_number() over (partition by order_status order by amount nulls last, id) = 1
),
metrics as (
    select
        *,
        avg(amount) over (order by order_date, id rows between 5 preceding and 1 preceding) as amount_avg_5_prior,
        round(amount / sum(amount) over (partition by order_status), 4) as amount_share
    from deduped
    where amount >= 470
),
deduped_payments as (
    select *
    from metrics
    qualify rank() over (partition by order_status order by amount desc) = 1
)
select
    id,
    amount_level,
    method_filled,
    order_status,
    order_date,
    amount_share
from deduped_payments
order by id desc;
