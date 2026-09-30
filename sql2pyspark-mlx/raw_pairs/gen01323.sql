with latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, rank() over (partition by customer_id, status order by order_date desc) as rn
        from orders
    ) t
    where rn = 1
),
with_dims as (
    -- Attach customers attributes
    select
        s.id,
        s.customer_id,
        s.store_id,
        s.order_date,
        s.status,
        s.shipping_fee,
        s.rn,
        c.segment,
        c.age,
        c.city
    from latest s
    left join customers c
        on s.customer_id <=> c.id
),
scored as (
    -- Ranking and gap features
    select
        *,
        lag(shipping_fee) over (partition by city order by rn, id) as prev_shipping_fee,
        lead(shipping_fee) over (partition by city order by rn, id) as next_shipping_fee,
        lag(age) over (partition by city order by rn, id) as prev_age,
        lead(age) over (partition by city order by rn, id) as next_age,
        row_number() over (partition by city order by rn, id) as event_seq,
        dense_rank() over (partition by city order by age desc) as age_dense_rank
    from with_dims
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        status,
        shipping_fee,
        segment,
        city,
        prev_age,
        age_dense_rank,
        sum(event_seq) over (partition by store_id order by age_dense_rank, id rows between unbounded preceding and current row) as running_sum_event_seq,
        max(event_seq) over (partition by store_id order by age_dense_rank, id rows between unbounded preceding and current row) as running_max_event_seq,
        avg(event_seq) over (partition by store_id order by age_dense_rank, id rows between unbounded preceding and current row) as running_avg_event_seq
    from scored
)
-- Aggregate with conditional metrics
select
    case when prev_age >= 70 then 'PREMIUM' else 'STANDARD' end as prev_age_tier,
    status,
    count(*) as event_count,
    round(sum(running_avg_event_seq) / nullif(count(case when city = 'Chiang Mai' then 1 end), 0), 2) as running_avg_event_seq_per_chiang_mai,
    approx_percentile(running_avg_event_seq, 0.5) as running_avg_event_seq_p50
from metrics
group by grouping sets ((case when prev_age >= 70 then 'PREMIUM' else 'STANDARD' end, status), (case when prev_age >= 70 then 'PREMIUM' else 'STANDARD' end))
order by running_avg_event_seq_per_chiang_mai desc;
