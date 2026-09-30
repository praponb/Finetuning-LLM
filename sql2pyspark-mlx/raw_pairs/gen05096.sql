with lines as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        element
    from carts
    lateral view outer explode(coupon_codes) li as element
),
with_dims as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.element,
        p.country
    from lines a
    left join customers p
        on a.customer_id = p.id
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        case when element = 'SPRING10' then 1 else 0 end as is_spring10,
        substring(element, 1, 2) as element_prefix,
        coalesce(country, 'OTHER') as resolved_country
    from with_dims
    where customer_id is null
),
with_dims_carts as (
    -- Attach customers attributes
    select
        s.customer_id,
        s.country,
        s.is_spring10,
        s.element_prefix,
        b.age
    from filtered s
    left join customers b
        on s.customer_id = b.id
),
with_dims_carts_2 as (
    -- Attach customers attributes
    select
        src.*,
        d.email,
        d.name
    from with_dims_carts src
    left join customers d
        on src.customer_id <=> d.id
),
kept_keys as (
    -- Set intersection on keys
    select customer_id
    from with_dims_carts_2
    intersect
    select customer_id
    from carts
    where array_contains(coupon_codes, 'WELCOME5')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims_carts_2 s
    join kept_keys k on s.customer_id = k.customer_id
),
scored as (
    -- Window metrics over several different partitions
    select *, min(is_spring10) over (partition by customer_id, country) as customer_id_min_is_spring10
    from kept_rows
)
select
    customer_id,
    country,
    age,
    name,
    customer_id_min_is_spring10,
    case when name rlike '^(FRANK MILLER|bob smith)$' then 'PRIORITY' when name rlike '^(Alice Wong|  Carol Diaz)$' then 'CORE' else 'EXTENDED' end as name_zone
from scored
order by country desc
