with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        line.qty,
        line.price,
        line.promo
    from carts
    lateral view explode(items) li as line
),
enriched as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        b.credit_limit,
        b.signup_date,
        b.segment
    from flattened a
    left join customers b
        on a.customer_id <=> b.id
),
with_dims as (
    -- Attach customers attributes
    select
        s.id,
        s.customer_id,
        s.credit_limit,
        s.segment,
        d.name,
        d.city
    from enriched s
    left join customers d
        on s.customer_id = d.id
    where d.credit_limit between 7900 and 16000
),
joined as (
    -- Attach customers attributes
    select
        src.*,
        c.email,
        c.age
    from with_dims src
    join customers c
        on src.customer_id = c.id
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (order by age desc nulls last) as age_rank,
        cume_dist() over (order by age) as age_cume_dist
    from joined
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        segment,
        city,
        age,
        age_cume_dist,
        round(age_rank / sum(age_rank) over (partition by city), 4) as age_rank_share,
        rank() over (order by credit_limit) as credit_limit_rank,
        count(credit_limit) over (partition by segment) as segment_count_credit_limit,
        min(age) over (partition by city) as city_min_age
    from scored
),
enriched_carts as (
    -- Attach customers attributes
    select
        src.*,
        p.signup_date,
        p.name,
        p.age as customer_age
    from metrics src
    join customers p
        on src.customer_id <=> p.id
)
select
    customer_id,
    segment,
    city,
    age,
    age_cume_dist,
    age_rank_share,
    credit_limit_rank,
    city_min_age,
    signup_date
from enriched_carts
order by customer_id
