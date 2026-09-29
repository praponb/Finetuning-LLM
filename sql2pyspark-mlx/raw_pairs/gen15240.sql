with with_gaps as (
    select *, case when started_at - lag(started_at) over (partition by browser order by started_at, id) > interval '14' day then 1 else 0 end as is_new_burst
    from sessions
),
sessionized as (
    select *, sum(is_new_burst) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
select
    date_trunc('quarter', started_at) as quarter_start,
    case when island_id >= 8 then 'PREMIUM' else 'STANDARD' end as island_id_tier,
    date_trunc('year', started_at) as year_start,
    sum(case when browser = 'Edge' then is_new_burst else 0 end) as edge_is_new_burst,
    round(sum(is_new_burst) / nullif(count(case when country = 'US' then 1 end), 0), 2) as is_new_burst_per_us,
    sort_array(collect_set(browser)) as browser_list,
    percent_rank() over (partition by date_trunc('quarter', started_at) order by round(sum(is_new_burst) / nullif(count(case when country = 'US' then 1 end), 0), 2) desc) as is_new_burst_per_us_percent_rank
from sessionized
group by date_trunc('quarter', started_at), case when island_id >= 8 then 'PREMIUM' else 'STANDARD' end, date_trunc('year', started_at);
