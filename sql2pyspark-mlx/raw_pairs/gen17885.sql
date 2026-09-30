with base as (
    -- Keep recent, valid rows
    select
        id,
        name,
        email,
        city,
        segment,
        credit_limit,
        upper(city) as city_upper,
        coalesce(city, 'N/A') as city_clean
    from customers
    where segment is not null
),
with_latest as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select s.*, x.status
    from base s
    join lateral (
            select c.status
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) x on true
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (order by credit_limit desc, id) as event_seq,
        lead(credit_limit, 1) over (partition by segment order by credit_limit, id) as next_credit_limit
    from with_latest
    where status not in ('OPEN', 'FROZEN')
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from scored s
    where exists (
            select 1
            from accounts c
            where c.customer_id = s.id
              and c.customer_id in (2, 29)
        )
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        name,
        email,
        city,
        credit_limit,
        city_clean,
        status,
        event_seq,
        next_credit_limit,
        trim(city) as clean_city,
        coalesce(segment, 'NONE') as segment_filled
    from not_flagged
    where city is not null
),
with_latest_customers as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select s.*, top1.account_type
    from filtered s
    left join lateral (
            select c.account_type
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) top1 on true
),
grouped as (
    -- Multi-level aggregation
    select
        case when next_credit_limit >= 17400 then 'LARGE' else 'SMALL' end as next_credit_limit_tier,
        sort_array(collect_set(account_type)) as account_type_list,
        approx_percentile(credit_limit, 0.95) as credit_limit_p95,
        count(case when city = 'London' then 1 end) as london_count,
        size(collect_set(status)) as num_status
    from with_latest_customers
    group by 1
)
select
    next_credit_limit_tier,
    account_type_list,
    credit_limit_p95,
    london_count
from grouped
order by credit_limit_p95 desc
