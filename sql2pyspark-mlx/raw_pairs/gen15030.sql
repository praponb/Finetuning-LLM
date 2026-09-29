with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    from carts
    lateral view outer explode(attributes) e as attr_key, attr_value
),
enriched as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.created_at,
        a.coupon_codes,
        a.attr_value,
        d.credit_limit
    from lines a
    left join customers d
        on a.customer_id <=> d.id
),
base as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        attr_value,
        credit_limit,
        sort_array(coupon_codes) as sorted_coupon_codes
    from enriched
    where hour(created_at) between 8 and 13
),
enriched_carts as (
    -- Attach customers attributes
    select
        s.*,
        p.name,
        p.city
    from base s
    left join customers p
        on s.customer_id = p.id
),
scored as (
    -- Ranking and gap features
    select
        customer_id,
        attr_value,
        name,
        city,
        round(credit_limit / sum(credit_limit) over (partition by customer_id), 4) as credit_limit_share,
        stddev_samp(credit_limit) over (partition by city) as city_stddev_samp_credit_limit
    from enriched_carts
)
select
    attr_value,
    city,
    credit_limit_share,
    city_stddev_samp_credit_limit,
    case when city rlike '^(Singapore|Tokyo)$' then 'GROUP_B' when city rlike '^(Bangkok|Chiang Mai)$' then 'EXTENDED' else 'GROUP_A' end as city_zone
from scored
order by city_stddev_samp_credit_limit
