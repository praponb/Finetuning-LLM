with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        explode_outer(coupon_codes) as tag
    from carts
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        datediff(current_date(), created_at) as days_since_created_at,
        coalesce(tag, 'DIRECT') as resolved_tag
    from exploded
    where created_at > to_timestamp('2025-03-01 12:00:00')
      and tag not in ('VIP', 'WELCOME5')
),
enriched as (
    -- Attach customers attributes
    select
        src.*,
        p.age,
        p.country
    from valid src
    inner join customers p
        on src.customer_id = p.id
),
enriched_carts as (
    -- Running and moving aggregates
    select
        *,
        count(days_since_created_at) over (partition by country order by cast(created_at as bigint) range between 7200 preceding and current row) as days_since_created_at_count_2h,
        round(age / sum(age) over (partition by country), 4) as age_share
    from enriched
    where age < 49
)
select
    s.customer_id,
    s.created_at,
    s.tag,
    s.age,
    s.country,
    s.days_since_created_at_count_2h,
    s.age_share,
    case when country rlike '^(TH|US)$' then 'CORE' else 'EXTENDED' end as country_zone,
    (select sum(t2.discount) from orders t2 where t2.customer_id = s.customer_id) as sum_order_discount
from enriched_carts s
order by tag desc nulls last;
