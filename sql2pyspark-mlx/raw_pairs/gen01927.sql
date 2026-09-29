with base as (
    select
        *,
        upper(event_type) event_type_upper,
        floor(duration_sec) duration_sec_whole
    from web_events
    where event_time >= timestamp '2023-09-19 02:00:00'
      and event_time >= '2023-08-01'
)
select
    date_trunc('week', event_time) as week_start,
    approx_percentile(duration_sec, 0.95) as duration_sec_p95,
    bit_or(duration_sec) as duration_sec_bit_or,
    grouping_id() as grouping_id
from base
group by rollup (date_trunc('week', event_time))
order by duration_sec_p95 desc nulls last;
