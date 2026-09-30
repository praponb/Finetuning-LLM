with valid as (
    -- Basic cleansing and defaults
    select
        id,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        upper(email) as email_upper
    from customers
    where signup_date >= date '2023-04-26'
      and segment is not null
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from valid s
    where exists (
            select 1
            from accounts c
            where c.customer_id = s.id
              and c.account_type in ('CHECKING', 'LOAN')
        )
),
latest_match as (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    select s.*, top1.browser
    from eligible s
    join lateral (
            select c.browser
            from sessions c
            where c.customer_id = s.id
              and c.started_at <= s.signup_date
            order by c.started_at desc, c.id desc
            limit 1
        ) top1 on true
),
latest_match_customers as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.account_opened_date,
        x.account_balance
    from latest_match s
    left join lateral (
            select c.opened_date as account_opened_date, c.balance as account_balance
            from accounts c
            where c.customer_id = s.id
            order by c.opened_date desc, c.id desc
            limit 1
        ) x on true
),
grouped as (
    -- Aggregate with conditional metrics
    select
        case when age >= 62 then 'HIGH' else 'LOW' end as age_tier,
        date_trunc('month', account_opened_date) as activity_month,
        date_trunc('quarter', signup_date) as quarter_start,
        count(*) filter (where segment = 'Consumer') as consumer_rows,
        sort_array(collect_set(city)) as city_list,
        sort_array(collect_set(segment)) as segment_list
    from latest_match_customers
    group by grouping sets ((case when age >= 62 then 'HIGH' else 'LOW' end, date_trunc('month', account_opened_date), date_trunc('quarter', signup_date)), (case when age >= 62 then 'HIGH' else 'LOW' end, date_trunc('month', account_opened_date)), ())
)
select
    age_tier,
    activity_month,
    city_list,
    segment_list
from grouped;
