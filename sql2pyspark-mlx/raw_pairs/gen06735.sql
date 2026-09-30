with metrics as (
    select *, row_number() over (order by started_at desc, id) as event_seq
    from sessions
),
joined as (
    select s.*, p.country as customer_country
    from metrics s
    inner join customers p
        on s.customer_id <=> p.id
    where p.signup_date <= '2023-05-17'
),
with_prev as (
    select *, lag(started_at) over (partition by browser order by started_at, id) as prev_started_at
    from joined
),
flagged as (
    select *, case when prev_started_at is null or started_at > prev_started_at + interval '30' day then 1 else 0 end as new_group_flag
    from with_prev
),
sessionized as (
    select
        id,
        customer_id,
        browser,
        started_at,
        event_seq,
        customer_country,
        sum(new_group_flag) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
with_windows as (
    select
        *,
        ntile(3) over (order by burst_id, id) as burst_id_bucket,
        max(event_seq) over (partition by customer_country order by cast(started_at as bigint) range between 604800 preceding and current row) as event_seq_max_7d,
        lead(burst_id, 1) over (partition by customer_id order by event_seq, id) as next_burst_id,
        max(event_seq) over (order by cast(started_at as bigint) range between 7776000 preceding and current row) as event_seq_max_90d,
        dense_rank() over (order by burst_id desc) as burst_id_dense_rank
    from sessionized
)
select
    date_trunc('month', started_at) as activity_month,
    next_burst_id,
    date_trunc('quarter', started_at) as quarter_start,
    count(*) filter (where browser = 'Firefox') as firefox_rows,
    approx_percentile(burst_id, array(0.25, 0.5, 0.75)) as burst_id_quartiles,
    count(distinct id) as distinct_ids,
    count(*) filter (where customer_country = 'JP') as jp_rows,
    max(started_at) as last_seen,
    grouping_id() as gid
from with_windows
group by grouping sets ((date_trunc('month', started_at), next_burst_id, date_trunc('quarter', started_at)), (date_trunc('month', started_at), next_burst_id))
order by firefox_rows desc nulls first
