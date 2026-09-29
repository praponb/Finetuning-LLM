with filtered as (
    select
        *,
        substring(country, 1, 4) as country_prefix,
        cast(is_active as int) as is_active_flag
    from customers
    where is_active
      and segment is distinct from 'SMB'
)
select
    id,
    name,
    email,
    city,
    segment,
    signup_date,
    is_active,
    country_prefix,
    is_active_flag,
    sum(is_active_flag) over w_hist as last4_sum_is_active_flag,
    min(is_active_flag) over w_hist as last4_min_is_active_flag,
    count(is_active_flag) over w_hist as last4_count_is_active_flag,
    ntile(10) over w_recent as credit_limit_bucket
from filtered
where city in ('New York', 'London')
window w_hist as (partition by segment, city order by credit_limit, id rows between 3 preceding and current row), w_recent as (partition by segment order by credit_limit desc, id)
