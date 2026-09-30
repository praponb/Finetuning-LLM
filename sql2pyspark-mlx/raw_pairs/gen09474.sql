with exploded as (
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    from carts
    lateral view outer explode(attributes) t as key, value
),
enriched as (
    select
        s.*,
        p.age,
        p.email,
        p.name
    from exploded s
    left join customers p
        on s.customer_id = p.id
),
filtered as (
    select
        *,
        upper(trim(name)) as name_normalized,
        array_contains(coupon_codes, 'WELCOME5') as is_welcome5,
        coalesce(key, 'DIRECT') as key_filled
    from enriched
    where key is not null
),
cleaned as (
    select
        *,
        round(age * 100, 2) as age_scaled,
        coalesce(key, 'N/A') as resolved_key
    from filtered
    where key is not null
      and created_at > date '2025-03-01'
),
metrics as (
    select
        created_at,
        key,
        value,
        age,
        email,
        name,
        key_filled,
        age_scaled,
        resolved_key,
        sum(age) over w_running as age_sum_15m,
        count(age) over w_part as age_count_2h,
        dense_rank() over (partition by customer_id order by age desc nulls first) as age_dense_rank,
        percent_rank() over w_running_2 as age_scaled_percent_rank
    from cleaned
    window w_running as (partition by customer_id order by cast(created_at as bigint) range between 900 preceding and current row), w_part as (partition by key, customer_id order by cast(created_at as bigint) range between 7200 preceding and current row), w_running_2 as (order by age_scaled desc nulls first)
),
with_windows as (
    select
        *,
        min(age) over (partition by key) as key_min_age,
        round(age_dense_rank / sum(age_dense_rank) over (partition by key), 4) as age_dense_rank_share,
        dense_rank() over (partition by month(created_at), key order by age_scaled nulls first) as age_scaled_dense_rank
    from metrics
)
select
    key,
    date_trunc('hour', created_at) as hour_window,
    max(key_min_age) filter (where key = 'gift') as gift_max_key_min_age,
    min(created_at) as first_seen,
    min(created_at) as first_created_at
from with_windows
group by key, date_trunc('hour', created_at)
