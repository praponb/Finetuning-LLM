with filtered as (
    select
        id,
        name,
        email,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        age,
        trim(email) as clean_email,
        concat(city, ' ', name) as city_name
    from customers
    where segment is not null
      and is_active
),
latest as (
    select
        id,
        city,
        age,
        clean_email
    from (
        select *, row_number() over (partition by city order by credit_limit desc, id) as rank_in_group
        from filtered
    ) x
    where rank_in_group = 1
),
kept as (
    select s.*
    from latest s
    where exists (
            select 1
            from orders ch
            where ch.customer_id = s.id
              and ch.store_id is not null
        )
),
eligible as (
    select s.*
    from kept s
    where exists (
            select 1
            from accounts ch
            where ch.customer_id = s.id
              and ch.customer_id in (19, 22)
        )
),
latest_match as (
    select s.*, top1.device
    from eligible s
    left join lateral (
            select c.device
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) top1 on true
)
select
    city,
    case when age >= 65 then 'PREMIUM' else 'STANDARD' end as age_tier,
    sort_array(collect_set(device)) as device_list,
    percentile_approx(age, 0.9) as age_p90,
    count(distinct id) as distinct_ids
from latest_match
group by city, case when age >= 65 then 'PREMIUM' else 'STANDARD' end
having distinct_ids < 1000
order by city desc
