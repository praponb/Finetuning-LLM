with eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from stores s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date <= s.opened_date
        )
),
remaining_keys as (
    -- Set difference on keys
    select id, region
    from eligible
    except
    select id, region
    from stores
    where year(opened_date) >= 2025
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from eligible s
    inner join remaining_keys k on s.id = k.id and s.region = k.region
),
metrics as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over (partition by region order by sqft) as sqft_cume_dist,
        dense_rank() over (partition by city order by sqft desc nulls first) as sqft_dense_rank
    from verified
    where region is null
),
filtered as (
    -- Keep recent, valid rows
    select
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_cume_dist,
        sqft_dense_rank,
        case when city = 'Berlin' then 1 else 0 end as is_berlin
    from metrics
    where opened_date >= to_date('2023-03-13')
)
-- Native SQL PIVOT
select *
from (
    select
        region,
        city,
        sqft_dense_rank
    from filtered
) src
pivot (
    max(sqft_dense_rank)
    for city in ('Berlin', 'Bangkok')
);
