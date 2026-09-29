with lines as (
    select
        id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    from carts
    lateral view explode(attributes) e as attr_key, attr_value
),
matched_keys as (
    select id
    from lines
    except distinct
    select id
    from carts
    where customer_id in (12, 21, 25)
),
reconciled as (
    select s.*
    from lines s
    inner join matched_keys k on s.id = k.id
),
cleaned as (
    select
        *,
        initcap(trim(attr_key)) as attr_key_title,
        coalesce(attr_key, 'UNKNOWN') as attr_key_filled
    from reconciled
    where created_at > timestamp '2025-03-01 16:00:00'
      and attr_key is distinct from 'device'
),
valid as (
    select
        created_at,
        coupon_codes,
        attr_key,
        attr_value,
        attr_key_title,
        hour(created_at) as created_at_hour,
        coalesce(attr_key, 'N/A') as attr_key_filled_2
    from cleaned
    where created_at > date '2025-03-01'
      and attr_key in ('utm_source', 'gift', 'weight_kg')
),
cleaned_carts as (
    select
        created_at,
        attr_value,
        attr_key_title,
        created_at_hour,
        attr_key_filled_2,
        coalesce(created_at_hour, 0) as created_at_hour_filled
    from valid
    where created_at > date '2025-03-01'
      and attr_key is distinct from 'utm_source'
)
select
    date_trunc('day', created_at) as activity_day,
    date_trunc('hour', created_at) as hour_window,
    max(created_at) as last_seen,
    percentile_approx(created_at_hour_filled, array(0.25, 0.5, 0.75)) as created_at_hour_filled_quartiles,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as span_minutes,
    min(created_at) as first_seen,
    min(created_at) as first_seen_2,
    max(created_at) as ended_at
from cleaned_carts
group by date_trunc('day', created_at), date_trunc('hour', created_at)
having (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 >= 0
order by activity_day desc;
