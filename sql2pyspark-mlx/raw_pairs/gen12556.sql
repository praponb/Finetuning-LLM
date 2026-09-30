with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    from carts
    lateral view explode(items) e as item
),
enriched as (
    -- Attach customers attributes
    select a.*, c.age
    from exploded a
    left join customers c
        on a.customer_id = c.id
    where c.credit_limit between 5500 and 10200
),
joined as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.created_at,
        a.product_id,
        a.qty,
        a.age,
        c.credit_limit
    from enriched a
    left join customers c
        on a.customer_id = c.id
),
enriched_carts as (
    -- Attach customers attributes
    select src.*, p.email
    from joined src
    inner join customers p
        on src.customer_id = p.id
),
base as (
    -- Basic cleansing and defaults
    select
        customer_id,
        created_at,
        product_id,
        qty,
        age,
        credit_limit,
        email,
        hour(created_at) as created_at_hour,
        substring(email, 1, 4) as email_prefix
    from enriched_carts
    where created_at > to_timestamp('2025-03-02 00:00:00')
      and age between 38 and 39
),
grouped as (
    -- Multi-level aggregation
    select
        case when created_at_hour >= 74 then 'LARGE' else 'SMALL' end as created_at_hour_tier,
        count(*) as row_count,
        count(distinct customer_id) as distinct_customer_ids,
        count(*) as num_rows,
        percentile_approx(created_at_hour, array(0.25, 0.5, 0.75)) as created_at_hour_quartiles,
        count(*) as event_count,
        round(avg(credit_limit), 2) as avg_credit_limit
    from base
    group by 1
)
select
    created_at_hour_tier,
    row_count,
    distinct_customer_ids,
    num_rows,
    event_count,
    avg_credit_limit
from grouped
order by event_count desc, row_count
