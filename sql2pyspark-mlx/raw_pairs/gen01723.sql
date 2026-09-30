with valid as (
    -- Filter the raw rows and derive helper columns
    select *, trim(status) as clean_status
    from subscriptions
    where end_date > date '2023-09-18'
      and start_date <= '2024-02-01'
),
kept_keys as (
    -- Set difference on keys
    select customer_id
    from valid
    except
    select customer_id
    from subscriptions
    where end_date <= '2025-06-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    join kept_keys k on s.customer_id = k.customer_id
),
enriched as (
    -- Attach customers attributes
    select
        src.*,
        c.age,
        c.signup_date
    from verified src
    left join customers c
        on src.customer_id <=> c.id
),
filtered as (
    -- Keep recent, valid rows
    select
        customer_id,
        start_date,
        end_date,
        status,
        clean_status,
        age,
        signup_date,
        initcap(trim(status)) as status_title
    from enriched
    where start_date >= date '2023-06-18'
      and status not in ('PAUSED', 'ACTIVE', 'EXPIRED')
),
scored as (
    -- Running and moving aggregates
    select
        customer_id,
        status,
        age,
        signup_date,
        percent_rank() over (order by age asc nulls last) as age_percent_rank,
        rank() over (partition by status order by age desc nulls last) as age_rank
    from filtered
    where start_date < '2025-07-01'
)
select
    status,
    age,
    signup_date,
    age_percent_rank,
    case when status rlike '^(EXPIRED|PAUSED)$' then 'GROUP_B' when status rlike '^(ACTIVE|CANCELLED)$' then 'OTHER_ZONE' else 'PRIORITY' end as status_zone
from scored
