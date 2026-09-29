with with_windows as (
    select
        *,
        first_value(channel, true) over (partition by store_id order by order_date asc, id asc rows between unbounded preceding and current row) as first_known_channel,
        avg(amount) over (partition by store_id, status order by shipping_fee asc, id asc rows between 5 preceding and 1 preceding) as amount_avg_5_prior
    from orders
    where channel in ('mobile', 'web')
)
select
    s.id,
    s.customer_id,
    s.store_id,
    s.status,
    s.channel,
    s.amount,
    s.amount_avg_5_prior,
    (select max(t2.balance) from accounts t2 where t2.customer_id = s.customer_id) as max_account_balance
from with_windows s
order by amount_avg_5_prior desc nulls first, status desc
