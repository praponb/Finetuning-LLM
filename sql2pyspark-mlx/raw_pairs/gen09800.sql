with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        value
    from carts
    lateral view outer explode(coupon_codes) t as value
),
matched_keys as (
    -- Set intersection on keys
    select customer_id, id
    from exploded
    intersect
    select customer_id, id
    from carts
    where customer_id is not null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from exploded s
    join matched_keys k on s.customer_id = k.customer_id and s.id = k.id
),
joined as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        b.age,
        b.city,
        b.country
    from kept_rows e
    left join customers b
        on e.customer_id = b.id
),
joined_carts as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.country,
        d.city as customer_city,
        d.signup_date
    from joined a
    join customers d
        on a.customer_id = d.id
),
enriched as (
    -- Attach customers attributes
    select
        s.id,
        s.customer_id,
        s.country,
        c.country as customer_country
    from joined_carts s
    join customers c
        on s.customer_id <=> c.id
    where c.segment in ('Corporate', 'Consumer')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when country in ('US', 'SG') then 'Core' else 'Other' end as country_group,
        case when country = 'US' then 1 else 0 end as is_us,
        coalesce(customer_country, 'DIRECT') as customer_country_clean
    from enriched
    where customer_id is not null
      and customer_country != 'TH'
),
with_dims as (
    -- Attach customers attributes
    select
        s.id,
        s.customer_id,
        s.customer_country,
        s.country_group,
        s.is_us,
        b.credit_limit,
        b.name,
        b.age
    from cleaned s
    join customers b
        on s.customer_id = b.id
),
daily as (
    -- Grouped metrics
    select
        case when credit_limit >= 16700 then 'LARGE' else 'SMALL' end as credit_limit_tier,
        round(avg(credit_limit), 2) as avg_credit_limit,
        sort_array(collect_set(customer_country)) as customer_country_list,
        percentile_approx(age, 0.95) as age_p95,
        size(collect_set(customer_country)) as num_customer_countries
    from with_dims
    group by case when credit_limit >= 16700 then 'LARGE' else 'SMALL' end
)
select
    credit_limit_tier,
    customer_country_list,
    num_customer_countries
from daily
order by num_customer_countries, credit_limit_tier;
