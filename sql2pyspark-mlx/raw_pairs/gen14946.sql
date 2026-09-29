with with_gaps as (
    select *, case when lag(paid_at) over (partition by method order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by method order by paid_at, id) + interval '90' day then 1 else 0 end as is_new_burst
    from payments
),
islands as (
    select
        id,
        order_id,
        method,
        paid_at,
        status,
        sum(is_new_burst) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as session_id
    from with_gaps
)
select
    case when session_id >= 6 then 'LARGE' else 'SMALL' end as session_id_tier,
    round(avg(session_id), 2) as avg_session_id,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as duration_min,
    sum(case when method = 'promptpay' then session_id else 0 end) as promptpay_session_id,
    count(*) filter (where method = 'cash') as cash_rows,
    approx_percentile(session_id, 0.95) as session_id_p95,
    percentile_approx(session_id, array(0.25, 0.5, 0.75)) as session_id_quartiles
from islands
group by case when session_id >= 6 then 'LARGE' else 'SMALL' end
order by duration_min desc;
