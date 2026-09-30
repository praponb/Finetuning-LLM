with with_gaps as (
    select *, case when lag(paid_at) over (partition by method order by paid_at asc, id asc) is null
                 or paid_at > lag(paid_at) over (partition by method order by paid_at asc, id asc) + interval 7 days then 1 else 0 end as new_group_flag
    from payments
),
islands as (
    select *, sum(new_group_flag) over (partition by method order by paid_at asc, id asc rows between unbounded preceding and current row) as session_no
    from with_gaps
)
select
    method,
    approx_percentile(session_no, array(0.25, 0.5, 0.75)) as session_no_quartiles,
    avg(amount) filter (where method = 'card') as card_avg_amount,
    count(*) as event_count
from islands
group by 1
order by event_count desc;
