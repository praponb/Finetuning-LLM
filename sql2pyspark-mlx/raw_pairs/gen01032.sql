with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, round(revenue / units, 2) as revenue_per_unit
    from store_sales
    where year(sale_date) = 2025
),
with_dims as (
    -- Attach stores attributes
    select
        a.*,
        b.region,
        b.opened_date
    from cleaned a
    left join stores b
        on a.store_id = b.id
    where b.sqft >= 7517
),
matched_keys as (
    -- Set difference on keys
    select store_id, id
    from with_dims
    except distinct
    select store_id, id
    from store_sales
    where revenue < 2700
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    inner join matched_keys k on s.store_id = k.store_id and s.id = k.id
),
matched_keys_2 as (
    -- Set intersection on keys
    select id, product_id
    from verified
    intersect all
    select id, product_id
    from store_sales
    where store_id in (2, 5, 6)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    inner join matched_keys_2 k on s.id = k.id and s.product_id = k.product_id
),
base as (
    -- Keep recent, valid rows
    select
        *,
        round(units * 1.07, 2) as adjusted_units,
        lower(region) as region_lower
    from reconciled
    where sale_date >= date '2023-05-23'
      and product_id in (1, 8, 9)
)
select
    s.id,
    s.store_id,
    s.product_id,
    s.units,
    s.revenue,
    s.revenue_per_unit,
    s.opened_date,
    s.adjusted_units,
    (select min(s2.units) from store_sales s2 where s2.store_id = s.store_id) as min_store_sale_units
from base s
order by min_store_sale_units desc nulls first, units
