with enriched as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.account_type,
        src.balance,
        src.opened_date,
        src.status,
        p.city,
        p.age,
        p.signup_date
    from accounts src
    left join customers p
        on src.customer_id = p.id
),
joined as (
    -- Attach customers attributes
    select
        src.*,
        b.name,
        b.age as customer_age,
        b.credit_limit
    from enriched src
    inner join customers b
        on src.customer_id <=> b.id
    where b.country != 'JP'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat(city, ' - ', account_type) as city_account_type,
        coalesce(status, 'UNKNOWN') as resolved_status
    from joined
    where opened_date > to_date('2023-06-02')
      and customer_id in (12, 21, 30)
),
enriched_accounts as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        balance,
        opened_date,
        city,
        signup_date,
        name,
        customer_age,
        resolved_status,
        ntile(3) over (partition by account_type, city order by balance asc, id asc) as balance_bucket,
        last_value(status, true) over (partition by status order by age asc, id asc rows between unbounded preceding and current row) as last_known_status,
        avg(age) over (partition by status order by signup_date asc, id asc rows between 3 preceding and current row) as age_avg_3
    from cleaned
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched_accounts s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time >= s.opened_date
        )
),
with_dims as (
    -- Attach customers attributes
    select
        s.*,
        b.name as customer_name,
        b.city as customer_city,
        b.signup_date as customer_signup_date
    from not_flagged s
    left join customers b
        on s.customer_id = b.id
),
scored as (
    -- Ranking and gap features
    select
        id,
        balance,
        city,
        customer_age,
        balance_bucket,
        age_avg_3,
        customer_name,
        percent_rank() over w_running as customer_age_percent_rank,
        sum(customer_age) over (order by signup_date asc, id asc rows between unbounded preceding and current row) as running_customer_age,
        first_value(age_avg_3) ignore nulls over (partition by customer_city order by opened_date asc, id asc rows between unbounded preceding and current row) as first_known_age_avg_3,
        sum(customer_age) over (partition by customer_id, customer_city order by signup_date asc, id asc rows between unbounded preceding and current row) as cumulative_customer_age
    from with_dims
    window w_running as (order by customer_age asc nulls first)
)
select
    id,
    balance,
    city,
    balance_bucket,
    age_avg_3,
    customer_name,
    customer_age_percent_rank,
    running_customer_age,
    first_known_age_avg_3,
    case when city rlike '^(Tokyo|Singapore)$' then 'CORE' else 'EXTENDED' end as city_zone
from scored
order by running_customer_age asc nulls last
