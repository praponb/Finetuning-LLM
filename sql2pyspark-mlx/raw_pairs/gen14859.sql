with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        code
    from carts
    lateral view explode(coupon_codes) e as code
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        substring(code, 1, 4) as code_prefix,
        lower(code) as code_lower,
        coalesce(code, 'DIRECT') as resolved_code
    from flattened
    where customer_id = 32
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, case when code in ('BUNDLE', 'SPRING10') then 'Group A' else 'Other' end as code_group
    from valid
    where customer_id = 9
),
enriched as (
    -- Attach customers attributes
    select
        e.*,
        d.segment,
        d.signup_date
    from filtered e
    left join customers d
        on e.customer_id = d.id
)
-- Multi-level aggregation
select
    segment,
    count(*) as num_rows,
    count_if(code = 'BUNDLE') as bundle_events,
    count(case when segment = 'SMB' then 1 end) as smb_count
from enriched
group by segment
