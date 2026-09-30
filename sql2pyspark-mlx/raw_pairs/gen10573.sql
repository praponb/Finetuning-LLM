with flagged as (
    select *, case when lag(started_at) over (partition by customer_id order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by customer_id order by started_at, id) + interval 90 days then 1 else 0 end as is_new_session
    from sessions
),
sessionized as (
    select *, sum(is_new_session) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as burst_id
    from flagged
)
select
    case when is_new_session >= 1 then 'HIGH' else 'LOW' end as is_new_session_tier,
    browser,
    burst_id,
    count(*) filter (where browser = 'Safari') as safari_rows,
    round(avg(burst_id), 2) as avg_burst_id,
    percentile_approx(burst_id, array(0.25, 0.5, 0.75)) as burst_id_quartiles,
    min(started_at) as started_at,
    count(*) as event_count,
    sort_array(collect_set(country)) as country_list,
    dense_rank() over (partition by case when is_new_session >= 1 then 'HIGH' else 'LOW' end order by round(avg(burst_id), 2) desc) as avg_burst_id_dense_rank
from sessionized
group by case when is_new_session >= 1 then 'HIGH' else 'LOW' end, browser, burst_id
having safari_rows > 1
order by avg_burst_id;
