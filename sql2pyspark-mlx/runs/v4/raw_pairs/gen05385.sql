with flattened as (
    -- Unnest the array into one row per element
    select id, explode(coupon_codes) as code
    from carts
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when code in ('BUNDLE', 'WELCOME5') then 'Core' else 'Other' end as code_group,
        substring(code, 1, 4) as code_prefix,
        coalesce(code, 'DIRECT') as code_filled
    from flattened
    where code is not null
),
cleaned_carts as (
    -- Basic cleansing and defaults
    select
        id,
        code,
        code_group,
        code_prefix,
        case when code in ('BUNDLE', 'WELCOME5') then 'Group A' else 'Other' end as code_group_2
    from cleaned
    where code is not null
      and code != 'BUNDLE'
)
-- Multi-level aggregation
select
    code,
    count(*) filter (where code = 'WELCOME5') as welcome5_rows,
    count(case when code = 'BUNDLE' then 1 end) as bundle_count
from cleaned_carts
group by code
order by welcome5_rows desc nulls last;
