with with_last as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.store_sale_units,
        top1.sale_date
    from stores s
    left join lateral (
            select c.units as store_sale_units, c.sale_date
            from store_sales c
            where c.store_id = s.id
            order by c.sale_date desc, c.id desc
            limit 1
        ) top1 on true
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_last s
    where exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date >= s.sale_date
        )
),
not_flagged_stores as (
    -- Semi-join via a correlated subquery
    select s.*
    from not_flagged s
    where exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.discount is null
        )
),
with_last_stores as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.region,
        s.opened_date,
        s.sqft,
        s.store_sale_units,
        latest.amount
    from not_flagged_stores s
    left join lateral (
            select c.amount
            from orders c
            where c.store_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) latest on true
),
kept_keys as (
    -- Set difference on keys
    select id, city
    from with_last_stores
    except
    select id, city
    from stores
    where sqft >= 13866
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_last_stores s
    join kept_keys k on s.id = k.id and s.city = k.city
),
kept_keys_2 as (
    -- Set intersection on keys
    select city, region
    from verified
    intersect all
    select city, region
    from stores
    where sqft <= 5643
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    join kept_keys_2 k on s.city = k.city and s.region = k.region
)
-- Native SQL PIVOT
select *
from (
    select
        city,
        region,
        amount
    from reconciled
) src
pivot (
    sum(amount)
    for region in ('South', 'Central', 'North')
)
