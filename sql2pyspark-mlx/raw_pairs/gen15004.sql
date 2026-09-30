with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        key,
        attr_value
    from carts
    lateral view explode(attributes) t as key, attr_value
),
with_dims as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.coupon_codes,
        src.key,
        p.credit_limit,
        p.segment,
        p.country
    from exploded src
    inner join customers p
        on src.customer_id <=> p.id
    where p.age between 41 and 62
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by key order by credit_limit desc, id, key) as latest_rank
        from with_dims
    ) ranked
    where latest_rank = 1
),
with_dims_carts as (
    -- Attach customers attributes
    select
        s.*,
        d.city,
        d.name,
        d.age
    from ranked s
    left join customers d
        on s.customer_id = d.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        coupon_codes,
        key,
        credit_limit,
        segment,
        country,
        latest_rank,
        city,
        name,
        avg(age) over (partition by segment, key order by age, id, key rows between 3 preceding and current row) as last4_avg_age,
        sum(age) over (partition by segment, key order by age, id, key rows between 3 preceding and current row) as last4_sum_age,
        min(age) over (partition by segment, key order by age, id, key rows between 3 preceding and current row) as last4_min_age
    from with_dims_carts
),
metrics as (
    -- Ranking and gap features
    select
        id,
        coupon_codes,
        key,
        credit_limit,
        city,
        last_value(city) ignore nulls over (partition by segment, key order by latest_rank, id, key rows between unbounded preceding and current row) as last_known_city,
        last_value(last4_min_age, true) over (order by last4_min_age, id, key rows between unbounded preceding and current row) as last_known_last4_min_age,
        lead(credit_limit, 1) over (partition by key order by credit_limit, id, key) as next_credit_limit
    from with_windows
)
-- Grouped metrics
select
    case when last_known_last4_min_age >= 66 then 'LARGE' else 'SMALL' end as last_known_last4_min_age_tier,
    case when credit_limit >= 16800 then 'HIGH' else 'LOW' end as credit_limit_tier,
    city,
    count(case when key = 'utm_source' then 1 end) as utm_source_count,
    sum(case when city = 'New York' then credit_limit else 0 end) as new_york_credit_limit,
    avg(next_credit_limit) filter (where key = 'weight_kg') as weight_kg_avg_next_credit_limit,
    size(collect_set(key)) as num_keys,
    sum(last_known_last4_min_age) filter (where city = 'Chiang Mai') as chiang_mai_total_last_known_last4_min_age
from metrics
group by cube (case when last_known_last4_min_age >= 66 then 'LARGE' else 'SMALL' end, case when credit_limit >= 16800 then 'HIGH' else 'LOW' end, city)
