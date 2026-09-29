with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        name,
        email,
        city,
        is_active,
        credit_limit,
        age,
        initcap(trim(city)) as city_title,
        regexp_extract(email, '@(.+)$', 1) as email_domain
    from customers
    where segment is not null
),
latest_match as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.account_opened_date,
        x.account_status
    from cleaned s
    left join lateral (
            select c.opened_date as account_opened_date, c.status as account_status
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) x on true
),
with_last as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.name,
        s.email,
        s.city,
        s.is_active,
        s.credit_limit,
        s.account_status,
        x.account_opened_date_2,
        x.account_account_type
    from latest_match s
    left join lateral (
            select c.opened_date as account_opened_date_2, c.account_type as account_account_type
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) x on true
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from with_last
    qualify row_number() over (partition by account_status, city order by credit_limit desc, id asc) = 1
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        city,
        credit_limit
    from ranked
    qualify row_number() over (partition by account_status order by account_opened_date_2 desc nulls last, id asc) = 1
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from latest s
    where exists (
            select 1
            from orders x
            where x.customer_id = s.id
              and x.status = 'CANCELLED'
        )
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from eligible s
    where exists (
            select 1
            from sessions ch
            where ch.customer_id = s.id
              and ch.country is null
        )
)
-- Grouped metrics
select
    city,
    case when credit_limit >= 17600 then 'HIGH' else 'LOW' end as credit_limit_tier,
    sum(credit_limit) filter (where city = 'Berlin') as berlin_total_credit_limit,
    count_if(city = 'Tokyo') as tokyo_events,
    sort_array(collect_set(city)) as city_list,
    round(sum(credit_limit) / nullif(count(case when city = 'New York' then 1 end), 0), 2) as credit_limit_per_new_york,
    percent_rank() over (partition by city order by count_if(city = 'Tokyo') desc) as tokyo_events_percent_rank
from not_flagged
group by city, case when credit_limit >= 17600 then 'HIGH' else 'LOW' end
order by berlin_total_credit_limit desc;
