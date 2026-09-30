with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by order_id order by paid_at, id) > interval '90' day then 1 else 0 end as new_group_flag
    from payments
)
-- Running sum of the flag = session number
select *, sum(new_group_flag) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as session_id
from gaps;
