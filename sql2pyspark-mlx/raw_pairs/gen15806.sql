with filtered as (
    -- Keep recent, valid rows
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        coalesce(store_name, 'N/A') as store_name_filled,
        round(sqft, 1) as sqft_rounded
    from stores
    where opened_date > date '2023-10-06'
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.customer_id in (10, 12, 15)
        )
),
matched_keys as (
    -- Set difference on keys
    select region, id
    from eligible
    except distinct
    select region, id
    from stores
    where sqft <> 9869
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from eligible s
    inner join matched_keys k on s.region = k.region and s.id = k.id
),
scored as (
    -- Ranking and gap features
    select *, cume_dist() over w_hist as sqft_rounded_cume_dist
    from verified
    where store_name like 'Air%'
    window w_hist as (partition by region order by sqft_rounded desc nulls first)
),
filtered_stores as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        store_name_filled,
        sqft_rounded,
        replace(store_name, ' ', '') as store_name_slug
    from scored
    where region is not null
      and sqft_rounded <= 46
),
eligible_stores as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered_stores s
    where not exists (
            select 1
            from store_sales ch
            where ch.store_id = s.id
              and ch.store_id in (1, 3, 5)
        )
)
select
    id,
    store_name,
    city,
    region,
    sqft_rounded,
    store_name_slug
from eligible_stores
order by id desc, region;
