with with_gaps as (
    select *, case when started_at - lag(started_at) over (partition by device order by started_at, id) > interval '90' day then 1 else 0 end as is_new_burst
    from sessions
),
sessionized as (
    select *, sum(is_new_burst) over (partition by device order by started_at, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
)
select
    date_trunc('year', started_at) as year_start,
    count(*) filter (where browser = 'Firefox') as firefox_rows,
    count_if(device = 'mobile') as mobile_events
from sessionized
group by date_trunc('year', started_at)
order by mobile_events
