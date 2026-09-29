with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    from carts
    lateral view explode(attributes) x as k, v
),
enriched as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.created_at,
        e.k,
        b.age
    from flattened e
    left join customers b
        on e.customer_id = b.id
    where b.credit_limit > 8400
),
scored as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        created_at,
        k,
        age,
        round(age / sum(age) over (partition by to_date(created_at), customer_id), 4) as age_share,
        first_value(k) ignore nulls over (partition by k order by created_at, id, k rows between unbounded preceding and current row) as first_known_k
    from enriched
),
scored_carts as (
    -- Window metrics over several different partitions
    select
        id,
        k,
        age,
        first_known_k,
        avg(age) over (partition by customer_id order by age_share, id, k rows between 6 preceding and 1 preceding) as age_avg_6_prior,
        ntile(5) over (partition by customer_id order by age desc, id, k) as age_bucket
    from scored
)
-- Multi-level aggregation
select
    case when age_bucket >= 9 then 'LARGE' else 'SMALL' end as age_bucket_tier,
    sum(case when k = 'device' then age else 0 end) as device_age,
    percentile_approx(age, 0.9) as age_p90,
    count_if(k = 'weight_kg') as weight_kg_events,
    count(distinct k) as distinct_ks,
    round(avg(age_avg_6_prior), 2) as avg_age_avg_6_prior,
    count_if(k = 'utm_source') as utm_source_events
from scored_carts
group by case when age_bucket >= 9 then 'LARGE' else 'SMALL' end
order by avg_age_avg_6_prior nulls last
