with kept_keys as (
    -- Set difference on keys
    select account_type
    from accounts
    except all
    select account_type
    from accounts
    where status in ('CLOSED', 'FROZEN', 'OPEN')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from accounts s
    inner join kept_keys k on s.account_type = k.account_type
),
joined as (
    -- Attach customers attributes
    select
        a.*,
        d.city,
        d.signup_date
    from kept_rows a
    left join customers d
        on a.customer_id = d.id
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from joined s
    where exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time >= s.signup_date
        )
),
joined_accounts as (
    -- Attach customers attributes
    select
        src.customer_id,
        src.account_type,
        src.balance,
        src.status,
        src.city,
        d.segment,
        d.age,
        d.country
    from eligible src
    left join customers d
        on src.customer_id = d.id
    where lower(d.name) like '%avi%'
)
-- Aggregate with conditional metrics
select
    status,
    case when age >= 57 then 'PREMIUM' else 'STANDARD' end as age_tier,
    round(sum(age) / nullif(count(case when account_type = 'CREDIT' then 1 end), 0), 2) as age_per_credit,
    count(distinct customer_id) as distinct_customer_ids,
    avg(age) as avg_age,
    stddev_samp(balance) as balance_stddev,
    count(distinct city) as distinct_cities,
    sum(case when city = 'Singapore' then age else 0 end) as singapore_age,
    dense_rank() over (partition by status order by stddev_samp(balance) desc) as balance_stddev_dense_rank
from joined_accounts
group by status, case when age >= 57 then 'PREMIUM' else 'STANDARD' end
order by distinct_cities asc;
