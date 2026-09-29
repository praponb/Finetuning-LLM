with valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        account_type,
        opened_date,
        last_day(opened_date) as opened_date_month_end,
        coalesce(status, 'DIRECT') as resolved_status
    from accounts
    where opened_date < '2024-04-28'
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.txn_time < s.opened_date
        )
),
remaining_keys as (
    -- Set difference on keys
    select customer_id, id
    from not_flagged
    except distinct
    select customer_id, id
    from accounts
    where account_type = 'CHECKING'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from not_flagged s
    inner join remaining_keys k on s.customer_id = k.customer_id and s.id = k.id
),
joined as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.opened_date_month_end,
        p.name,
        p.email,
        p.age
    from verified e
    left join customers p
        on e.customer_id = p.id
),
with_dims as (
    -- Attach customers attributes
    select
        src.customer_id,
        src.name,
        src.email,
        c.email as customer_email,
        c.name as customer_name,
        c.signup_date
    from joined src
    left join customers c
        on src.customer_id = c.id
    where c.age < 35
),
with_dims_accounts as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.name,
        s.email,
        s.customer_name,
        s.signup_date,
        b.segment,
        b.age,
        b.city
    from with_dims s
    left join customers b
        on s.customer_id = b.id
)
-- Multi-level aggregation
select
    date_trunc('month', signup_date) as activity_month,
    round(sum(age) / nullif(count(case when segment = 'Consumer' then 1 end), 0), 2) as age_per_consumer,
    count(case when city = 'London' then 1 end) as london_count
from with_dims_accounts
group by 1;
