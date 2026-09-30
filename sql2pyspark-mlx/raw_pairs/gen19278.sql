with base as (
    select
        *,
        length(device) as device_length,
        coalesce(browser, 'DIRECT') as resolved_browser
    from sessions
    where browser is not distinct from 'Chrome'
      and started_at > timestamp '2023-08-28 02:00:00'
)
select
    *,
    lag(device_length, 1) over (partition by browser order by started_at, id) as prev_device_length,
    first_value(device_length) ignore nulls over (partition by browser order by started_at, id rows between unbounded preceding and current row) as first_known_device_length
from base
where customer_id in (23, 29);
