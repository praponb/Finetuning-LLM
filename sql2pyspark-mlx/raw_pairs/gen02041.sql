with enriched as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.plan,
        src.start_date,
        src.status,
        p.country,
        p.name
    from subscriptions src
    inner join customers p
        on src.customer_id <=> p.id
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        start_date,
        status,
        name
    from (
        select *, row_number() over (partition by country order by start_date desc nulls first, id) as dedup_rank
        from enriched
    ) t
    where dedup_rank = 1
),
enriched_subscriptions as (
    -- Running and moving aggregates
    select
        *,
        row_number() over w_part as row_seq,
        row_number() over w_recent as seq_no,
        row_number() over w_ordered as event_seq,
        row_number() over w_part as event_seq_2
    from latest
    where name not like '%Rossi%'
    window w_part as (partition by status order by start_date desc, id), w_recent as (partition by status order by start_date, id), w_ordered as (partition by month(start_date), status order by start_date desc, id)
),
enriched_subscriptions_2 as (
    -- Window metrics over several different partitions
    select
        id,
        name,
        row_seq,
        event_seq,
        event_seq_2,
        lag(seq_no) over (partition by status order by start_date, id) as prev_seq_no,
        lead(seq_no) over (partition by status order by start_date, id) as next_seq_no,
        lead(row_seq) over (partition by status order by start_date, id) as next_row_seq,
        lag(row_seq) over (partition by status order by start_date, id) as prev_row_seq,
        row_number() over (partition by status order by start_date, id) as seq_no_2,
        round(row_seq / sum(row_seq) over (partition by month(start_date)), 4) as row_seq_share
    from enriched_subscriptions
),
valid as (
    -- Keep recent, valid rows
    select *, round(next_seq_no, 1) as next_seq_no_rounded
    from enriched_subscriptions_2
    where row_seq is not null
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(5) over w_running as row_seq_bucket,
        ntile(3) over w_ordered as row_seq_share_bucket,
        row_number() over (order by next_seq_no desc, id) as seq_no,
        last_value(row_seq, true) over (order by next_seq_no_rounded, id rows between unbounded preceding and current row) as last_known_row_seq
    from valid
    window w_running as (order by row_seq, id), w_ordered as (order by row_seq_share desc, id)
),
with_windows_subscriptions as (
    -- Running and moving aggregates
    select
        *,
        ntile(3) over (order by row_seq_share, id) as row_seq_share_bucket_2,
        rank() over (order by seq_no_2 desc nulls last) as seq_no_2_rank,
        lag(row_seq_bucket, 1) over (order by row_seq_share_bucket, id) as prev_row_seq_bucket
    from with_windows
)
-- Grouped metrics
select
    case when prev_seq_no >= 8 then 'PREMIUM' else 'STANDARD' end as prev_seq_no_tier,
    prev_seq_no,
    count(*) as num_rows,
    percentile_approx(row_seq_share_bucket, 0.5) as row_seq_share_bucket_p50
from with_windows_subscriptions
group by case when prev_seq_no >= 8 then 'PREMIUM' else 'STANDARD' end, prev_seq_no
order by prev_seq_no desc
