with with_windows as (
    select
        *,
        avg(credit_limit) over w_hist as running_avg_credit_limit,
        count(credit_limit) over w_hist as running_count_credit_limit,
        min(credit_limit) over w_hist as running_min_credit_limit,
        max(credit_limit) over w_hist as running_max_credit_limit,
        cume_dist() over (partition by segment order by credit_limit nulls first) as credit_limit_cume_dist
    from customers
    window w_hist as (partition by country, segment order by age, id rows between unbounded preceding and current row)
)
select
    id,
    name,
    email,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    case when segment rlike '^(SMB|Corporate)$' then 'PRIORITY' else 'OTHER_ZONE' end as segment_zone
from with_windows
order by signup_date nulls first
