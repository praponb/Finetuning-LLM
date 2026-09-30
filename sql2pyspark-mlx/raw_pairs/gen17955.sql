with valid as (
    -- Keep recent, valid rows
    select
        *,
        lower(store_name) as store_name_lower,
        trim(store_name) as clean_store_name
    from stores
    where region = 'Central'
),
remaining_keys as (
    -- Set difference on keys
    select id
    from valid
    except distinct
    select id
    from stores
    where sqft > 14779
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join remaining_keys k on s.id = k.id
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from reconciled s
    where exists (
            select 1
            from store_sales x
            where x.store_id = s.id
              and x.sale_date >= s.opened_date
        )
),
matched_keys as (
    -- Set difference on keys
    select city
    from not_flagged
    except
    select city
    from stores
    where city in ('London', 'Bangkok')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from not_flagged s
    inner join matched_keys k on s.city = k.city
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified s
    where not exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.product_id in (2, 4, 6, 10)
        )
)
select
    id,
    store_name,
    region,
    opened_date,
    sqft,
    clean_store_name
from eligible
order by region desc nulls first;
