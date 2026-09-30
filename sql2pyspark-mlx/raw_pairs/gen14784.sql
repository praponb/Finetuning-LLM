with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        key,
        attr_value
    from carts
    lateral view outer explode(attributes) e as key, attr_value
),
joined as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.attr_value,
        b.credit_limit,
        b.email,
        b.name
    from exploded a
    left join customers b
        on a.customer_id = b.id
    where b.country = 'SG'
),
kept_keys as (
    -- Set intersection on keys
    select id, customer_id
    from joined
    intersect
    select id, customer_id
    from carts
    where to_date(created_at) >= '2025-01-22'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    join kept_keys k on s.id = k.id and s.customer_id = k.customer_id
),
matched_keys as (
    -- Set difference on keys
    select id, customer_id
    from reconciled
    except
    select id, customer_id
    from carts
    where created_at < '2025-06-01'
),
reconciled_carts as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from reconciled s
    join matched_keys k on s.id = k.id and s.customer_id = k.customer_id
),
matched_keys_2 as (
    -- Set intersection on keys
    select id
    from reconciled_carts
    intersect
    select id
    from carts
    where customer_id in (22, 34)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from reconciled_carts s
    join matched_keys_2 k on s.id = k.id
),
enriched as (
    -- Attach customers attributes
    select
        a.*,
        d.credit_limit as customer_credit_limit,
        d.segment
    from kept_rows a
    join customers d
        on a.customer_id <=> d.id
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        attr_value,
        credit_limit,
        email,
        customer_credit_limit,
        cast(customer_credit_limit as int) as customer_credit_limit_int
    from enriched
    where segment is not null
      and segment is distinct from 'SMB'
)
-- Join a dimension and aggregate
select
    dim.country,
    count(*) as num_rows,
    min(src.customer_credit_limit_int) as min_customer_credit_limit_int,
    grouping_id() as gid
from valid src
join customers dim on src.customer_id = dim.id
group by rollup (dim.country);
