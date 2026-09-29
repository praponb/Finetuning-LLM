with filtered as (
    select *, last_day(signup_date) as signup_date_month_end
    from customers
    where segment is not null
),
enriched as (
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        signup_date_month_end,
        corr(age, credit_limit) over (partition by city) as corr_age_credit_limit,
        lead(credit_limit, 1) over (order by signup_date, id) as next_credit_limit,
        max(age) over (partition by city, country) as city_max_age
    from filtered
),
kept as (
    select s.*
    from enriched s
    where not exists (
            select 1
            from sessions x
            where x.customer_id = s.id
              and x.customer_id in (14, 26)
        )
),
latest_match as (
    select
        s.*,
        latest.device,
        latest.session_browser
    from kept s
    left join lateral (
            select c.device, c.browser as session_browser
            from sessions c
            where c.customer_id = s.id
              and c.started_at <= s.signup_date
            order by c.started_at desc, c.id desc
            limit 1
        ) latest on true
)
select
    case when corr_age_credit_limit >= 0.58 then 'LARGE' else 'SMALL' end as corr_age_credit_limit_tier,
    case when age >= 56 then 'PREMIUM' else 'STANDARD' end as age_tier,
    segment,
    count(distinct id) as distinct_ids,
    count(case when session_browser = 'Chrome' then 1 end) as chrome_count
from latest_match
group by corr_age_credit_limit_tier, age_tier, segment
having distinct_ids >= 1
