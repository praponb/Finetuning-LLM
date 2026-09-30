with filtered as (
    select *, to_date(started_at) as started_at_date
    from sessions
    where started_at > timestamp '2023-02-17 23:00:00'
      and customer_id is null
),
flagged as (
    select *, case when lag(started_at) over (partition by country order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by country order by started_at, id) + interval '14' day then 1 else 0 end as is_new_burst
    from filtered
),
sessionized as (
    select *, sum(is_new_burst) over (partition by country order by started_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
)
select
    device,
    date_trunc('week', started_at) as week_start,
    date_trunc('month', started_at) as activity_month,
    approx_percentile(burst_id, 0.5) as burst_id_p50,
    count(*) filter (where device = 'tablet') as tablet_rows,
    grouping_id() as gid
from sessionized
group by rollup (device, date_trunc('week', started_at), date_trunc('month', started_at))
order by week_start desc nulls last;
