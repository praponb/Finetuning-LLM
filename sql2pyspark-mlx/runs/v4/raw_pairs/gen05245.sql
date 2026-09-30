with kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from suppliers s
    where exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.price < 320
        )
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        round(rating / sum(rating) over (partition by country, city), 4) as rating_share,
        ntile(10) over (order by rating desc, id) as rating_bucket
    from kept
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        supplier_name,
        rating,
        rating_bucket,
        round(rating_bucket * 0.85, 2) as adjusted_rating_bucket
    from with_windows
    where rating > 2.8
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from valid s
    where exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.cost > 230
        )
),
with_windows_suppliers as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(5) over (order by adjusted_rating_bucket desc, id) as adjusted_rating_bucket_bucket,
        last_value(adjusted_rating_bucket, true) over (order by rating_bucket, id rows between unbounded preceding and current row) as last_known_adjusted_rating_bucket
    from eligible
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from with_windows_suppliers
    intersect all
    select id
    from suppliers
    where country not in ('UK', 'US')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows_suppliers s
    inner join remaining_keys k on s.id = k.id
),
kept_suppliers as (
    -- Semi-join via a correlated subquery
    select s.*
    from kept_rows s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.brand in ('Initech', 'Stark', 'Acme')
        )
),
report as (
    select
        id,
        supplier_name,
        rating,
        rating_bucket,
        adjusted_rating_bucket,
        adjusted_rating_bucket_bucket,
        last_known_adjusted_rating_bucket
    from kept_suppliers
    order by adjusted_rating_bucket nulls last, adjusted_rating_bucket_bucket
)
select
    id,
    supplier_name,
    rating,
    rating_bucket,
    adjusted_rating_bucket,
    last_known_adjusted_rating_bucket,
    case when supplier_name rlike '^(Lion City Imports|Pacific Wholesale)$' then 'GROUP_A' else 'GROUP_B' end as supplier_name_zone
from report
