with with_prev as (
    select *, lag(paid_at) over (partition by order_id order by paid_at, id) as prev_paid_at
    from payments
),
with_gaps as (
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval 90 days then 1 else 0 end as is_new_burst
    from with_prev
),
sessionized as (
    select *, sum(is_new_burst) over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
)
select
    case when burst_id >= 7 then 'PREMIUM' else 'STANDARD' end as burst_id_tier,
    case when is_new_burst >= 1 then 'LARGE' else 'SMALL' end as is_new_burst_tier,
    sum(is_new_burst) filter (where method = 'bank_transfer') as bank_transfer_total_is_new_burst,
    count(*) as num_rows,
    approx_percentile(is_new_burst, array(0.25, 0.5, 0.75)) as is_new_burst_quartiles,
    stddev_samp(amount) as amount_stddev
from sessionized
group by case when burst_id >= 7 then 'PREMIUM' else 'STANDARD' end, case when is_new_burst >= 1 then 'LARGE' else 'SMALL' end
order by is_new_burst_tier desc
