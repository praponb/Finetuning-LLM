with filtered as (
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        floor(discount) as discount_whole,
        coalesce(channel, 'OTHER') as channel_clean
    from orders
    where order_date > date '2024-04-17'
      and store_id is not null
      and month(order_date) in (5, 6, 11)
),
enriched as (
    select
        *,
        round(amount / sum(amount) over (partition by store_id, channel), 4) as amount_share,
        row_number() over (order by order_date, id) as event_seq
    from filtered
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by customer_id order by amount desc, id) as rn
        from enriched
    ) ranked
    where rn = 1
)
select
    date_trunc('year', order_date) as year_start,
    date_trunc('week', order_date) as week_start,
    percentile_approx(event_seq, 0.5) as event_seq_p50,
    round(sum(amount) / nullif(count(case when status = 'RETURNED' then 1 end), 0), 2) as amount_per_returned,
    percentile_approx(event_seq, array(0.25, 0.5, 0.75)) as event_seq_quartiles
from ranked
group by date_trunc('year', order_date), date_trunc('week', order_date)
order by amount_per_returned desc nulls first
