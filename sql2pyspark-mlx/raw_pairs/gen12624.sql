with enriched as (
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        lead(cost, 1) over (partition by status order by shipped_date, id) as next_cost,
        sum(cost) over w_recent as order_id_sum_cost,
        first_value(cost, true) over (partition by carrier, status order by cost, id rows between unbounded preceding and current row) as first_known_cost,
        ntile(4) over (partition by carrier order by cost desc, id) as cost_bucket,
        row_number() over w_hist as seq_no
    from shipments
    window w_recent as (partition by order_id), w_hist as (partition by month(shipped_date), carrier order by cost desc, id)
),
with_dims as (
    select
        src.*,
        p.status,
        p.shipping_fee,
        p.channel
    from enriched src
    inner join orders p
        on src.order_id = p.id
    where p.status = 'COMPLETED'
),
base as (
    select
        *,
        round(cost, 0) as cost_rounded,
        case when next_cost >= 99 then 'High' when next_cost >= 38 then 'Medium' else 'Low' end as next_cost_tier
    from with_dims
    where shipped_date > date '2023-09-08'
      and channel is distinct from 'partner'
),
filtered as (
    select
        *,
        coalesce(cost, 0) as cost_filled,
        coalesce(channel, 'N/A') as channel_clean
    from base
    where shipped_date >= date '2023-08-22'
      and next_cost between 36 and 73
),
with_dims_shipments as (
    select
        e.id,
        e.order_id,
        e.shipped_date,
        e.delivered_date,
        e.cost,
        e.next_cost,
        e.order_id_sum_cost,
        e.first_known_cost,
        e.seq_no,
        e.channel,
        e.cost_filled,
        b.discount
    from filtered e
    left join orders b
        on e.order_id <=> b.id
),
matched as (
    select
        a.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from with_dims_shipments a
    left join promotions pr
        on a.channel = pr.channel
       and a.shipped_date between pr.starts_on and pr.ends_on
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by promo_code, order_id order by promo_id desc nulls last, id, promo_id) as dedup_rank
        from matched
    ) x
    where dedup_rank = 1
)
select
    s.id,
    s.order_id,
    s.delivered_date,
    s.cost,
    s.next_cost,
    s.order_id_sum_cost,
    s.seq_no,
    s.channel,
    s.cost_filled,
    case when channel rlike '^(store|web)$' then 'OTHER_ZONE' when channel rlike '^(mobile|partner)$' then 'EXTENDED' else 'GROUP_B' end as channel_zone,
    (select sum(x.amount) from payments x where x.order_id = s.order_id) as sum_payment_amount
from ranked s
order by delivered_date desc
