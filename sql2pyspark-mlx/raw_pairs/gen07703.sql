with base as (
    -- Basic cleansing and defaults
    select
        *,
        round(shipping_fee / amount, 2) as amount_with_shipping,
        substring(channel, 1, 2) as channel_prefix
    from orders
    where channel is distinct from 'web'
      and amount <= 590
),
enriched as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.store_id,
        src.order_date,
        src.channel,
        src.amount,
        src.discount,
        src.amount_with_shipping,
        src.channel_prefix,
        b.age,
        b.city
    from base src
    left join customers b
        on src.customer_id = b.id
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by channel, store_id order by order_date) as row_num
    from enriched
    qualify row_num = 1
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from ranked s
    where exists (
            select 1
            from payments c
            where c.order_id = s.id
              and c.status = 'SUCCESS'
        )
),
metrics as (
    -- Ranking and gap features
    select
        id,
        channel,
        amount,
        discount,
        channel_prefix,
        row_num,
        lag(age) over (partition by customer_id order by amount_with_shipping, id) as prev_age,
        lead(age) over (partition by customer_id order by amount_with_shipping, id) as next_age,
        lag(amount_with_shipping) over (partition by customer_id order by amount_with_shipping, id) as prev_amount_with_shipping,
        row_number() over (partition by customer_id order by amount_with_shipping, id) as event_seq,
        avg(age) over (order by amount, id rows between 6 preceding and 1 preceding) as age_avg_6_prior
    from eligible
)
select
    id,
    amount,
    discount,
    channel_prefix,
    row_num,
    prev_age,
    next_age,
    prev_amount_with_shipping,
    event_seq
from metrics
