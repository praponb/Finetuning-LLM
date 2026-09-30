with with_gaps as (
    select *, case when lag(paid_at) over (partition by method order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by method order by paid_at, id) + interval 90 days then 1 else 0 end as is_new_burst
    from payments
)
select *, sum(is_new_burst) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as burst_id
from with_gaps
