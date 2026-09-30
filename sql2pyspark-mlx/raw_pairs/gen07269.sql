with ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by country, segment order by age nulls first, id) as rn
        from customers
    ) ranked
    where rn = 1
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from ranked s
    where not exists (
            select 1
            from sessions x
            where x.customer_id = s.id
              and to_date(x.started_at) >= '2024-06-01'
        )
),
cleaned as (
    -- Basic cleansing and defaults
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
        rn,
        greatest(credit_limit, age) greatest_credit_limit_age
    from kept
    where is_active
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        country,
        segment,
        is_active,
        credit_limit,
        rn,
        avg(greatest_credit_limit_age) over (partition by segment, city order by rn, id rows between unbounded preceding and current row) running_avg_greatest_credit_limit_age,
        max(greatest_credit_limit_age) over (partition by segment, city order by rn, id rows between unbounded preceding and current row) running_max_greatest_credit_limit_age,
        sum(greatest_credit_limit_age) over (partition by segment, city order by rn, id rows between unbounded preceding and current row) running_sum_greatest_credit_limit_age
    from cleaned
),
with_last as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select s.*, x.balance
    from enriched s
    left join lateral (
            select c.balance
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    segment,
    credit_limit,
    running_max_greatest_credit_limit_age,
    running_sum_greatest_credit_limit_age,
    balance,
    case when segment rlike '^(SMB|Corporate)$' then 'GROUP_A' when segment rlike '^(Consumer)$' then 'EXTENDED' else 'CORE' end as segment_zone
from with_last
order by balance desc nulls first, id;
