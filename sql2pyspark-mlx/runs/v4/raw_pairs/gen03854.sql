with joined as (
    -- Attach sessions attributes
    select
        s.*,
        b.device,
        b.browser,
        b.country
    from web_events s
    left join sessions b
        on s.session_id = b.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        device,
        browser,
        country,
        ntile(4) over (partition by session_id order by duration_sec, id) as duration_sec_bucket,
        avg(duration_sec) over w_hist as duration_sec_avg_90d,
        rank() over (partition by page order by duration_sec desc) as duration_sec_rank
    from joined
    window w_hist as (order by cast(event_time as bigint) range between 7776000 preceding and current row)
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by session_id order by event_time, id) as dedup_rank
    from with_windows
    qualify dedup_rank = 1
)
-- Grouped metrics
select
    date_trunc('year', event_time) as year_start,
    min(event_time) as started_at,
    round(sum(duration_sec_rank) / nullif(count(case when page = '/checkout' then 1 end), 0), 2) as duration_sec_rank_per_checkout,
    bit_or(duration_sec) as duration_sec_bit_or
from deduped
group by date_trunc('year', event_time);
