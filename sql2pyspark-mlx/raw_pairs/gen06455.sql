with lines as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        coupon_codes,
        k,
        v
    from carts
    lateral view outer explode(attributes) t as k, v
),
exploded as (
    -- Unnest the array into one row per element
    select
        id,
        k,
        v,
        element
    from lines
    lateral view outer explode(coupon_codes) e as element
),
filtered as (
    -- Keep recent, valid rows
    select *, lower(element) element_lower
    from exploded
    where element not in ('BUNDLE', 'SPRING10')
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when k in ('device', 'gift') then 'Group A' else 'Other' end k_group,
        length(k) k_length,
        coalesce(element, 'DIRECT') element_clean
    from filtered
    where k is not null
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from cleaned
    intersect all
    select id
    from carts
    where customer_id is null
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    join remaining_keys k on s.id = k.id
)
-- Grouped metrics
select
    element,
    k,
    count(distinct element) as distinct_elements,
    percentile_approx(k_length, 0.9) as k_length_p90,
    count(*) as num_rows
from verified
group by element, k
order by k desc nulls last;
