with exploded as (
    select
        id,
        customer_id,
        created_at,
        k,
        value
    from carts
    lateral view explode(attributes) t as k, value
),
with_windows as (
    select
        *,
        last_value(k) ignore nulls over (partition by to_date(created_at), k order by created_at, id, k rows between unbounded preceding and current row) as last_known_k,
        row_number() over (partition by customer_id order by created_at, id, k) as row_seq,
        row_number() over (order by created_at desc, id, k) as event_seq,
        first_value(k) ignore nulls over (order by created_at, id, k rows between unbounded preceding and current row) as first_known_k,
        row_number() over (order by created_at desc, id, k) as event_seq_2
    from exploded
),
enriched as (
    select
        *,
        lag(event_seq_2) over (partition by k order by event_seq_2, id, k) as prev_event_seq_2,
        lead(event_seq_2) over (partition by k order by event_seq_2, id, k) as next_event_seq_2,
        lead(event_seq) over (partition by k order by event_seq_2, id, k) as next_event_seq,
        lag(event_seq) over (partition by k order by event_seq_2, id, k) as prev_event_seq,
        row_number() over (partition by k order by event_seq_2, id, k) as row_seq_2
    from with_windows
),
ranked as (
    select *
    from enriched
    qualify row_number() over (partition by k order by next_event_seq desc nulls first, id, k) = 1
)
select
    date_trunc('day', created_at) as activity_day,
    case when prev_event_seq >= 7 then 'HIGH' else 'LOW' end as prev_event_seq_tier,
    customer_id,
    min(event_seq_2) as min_event_seq_2,
    size(collect_set(k)) as num_ks,
    avg(prev_event_seq) filter (where k = 'weight_kg') as weight_kg_avg_prev_event_seq,
    size(collect_set(k)) as num_ks_2,
    count(case when k = 'utm_source' then 1 end) as utm_source_count,
    count_if(k = 'weight_kg') as weight_kg_events
from ranked
group by activity_day, prev_event_seq_tier, customer_id
order by num_ks desc
