with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at asc, id asc) > interval '30' day then 1 else 0 end as is_new_burst
    from payments
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by status order by paid_at asc, id asc rows between unbounded preceding and current row) as session_id
    from with_gaps
)
-- Multi-level aggregation
select
    method,
    case when session_id >= 8 then 'LARGE' else 'SMALL' end as session_id_tier,
    sort_array(collect_set(status)) as status_list,
    sum(case when status = 'REFUNDED' then amount else 0 end) as refunded_amount,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as span_minutes,
    approx_percentile(session_id, array(0.25, 0.5, 0.75)) as session_id_quartiles
from sessions_payments
group by method, session_id_tier
order by span_minutes desc;
