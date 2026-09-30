with filtered as (
    select
        id,
        customer_id,
        length(device) as device_length
    from sessions
    where started_at >= date '2023-08-25'
      and country = 'TH'
)
select
    *,
    sum(device_length) over (partition by customer_id order by device_length, id rows between unbounded preceding and current row) as cumulative_device_length,
    stddev_samp(device_length) over w_part as customer_id_stddev_samp_device_length,
    rank() over (partition by customer_id order by device_length desc) as device_length_rank,
    avg(device_length) over w_part as customer_id_avg_device_length,
    cume_dist() over w_running as device_length_cume_dist
from filtered
window w_part as (partition by customer_id), w_running as (partition by customer_id order by device_length nulls last)
