with filtered as (
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        initcap(trim(status)) as status_title
    from shipments
    where shipped_date >= date '2023-12-13'
),
enriched as (
    select
        a.*,
        o.shipping_fee,
        o.amount,
        o.channel
    from filtered a
    left join orders o
        on a.order_id = o.id
),
ranked as (
    select *
    from enriched
    qualify row_number() over (partition by carrier order by amount desc, id) = 1
),
scored as (
    select
        *,
        avg(amount) over (partition by carrier order by delivered_date, id rows between 6 preceding and current row) as amount_avg_6,
        avg(amount) over (order by delivered_date, id rows between 6 preceding and current row) as amount_avg_6_2,
        ntile(4) over (partition by channel, carrier order by shipping_fee desc, id) as shipping_fee_bucket
    from ranked
),
totals as (
    select
        date_trunc('month', delivered_date) as activity_month,
        date_trunc('week', delivered_date) as week_start,
        round(avg(amount_avg_6), 2) as avg_amount_avg_6,
        count(distinct id) as distinct_ids,
        count(*) as event_count,
        dense_rank() over (partition by date_trunc('month', delivered_date) order by count(*) desc) as event_count_dense_rank
    from scored
    group by date_trunc('month', delivered_date), date_trunc('week', delivered_date)
    having count(*) < 1
)
select
    activity_month,
    week_start,
    avg_amount_avg_6,
    distinct_ids,
    event_count_dense_rank
from totals
order by distinct_ids desc;
