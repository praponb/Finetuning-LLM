with latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by city order by rating desc nulls first, id) as rn
    from suppliers
    qualify rn = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        country,
        city,
        rating,
        rn,
        lag(rating, 1, 0) over (partition by city order by rn, id) as prev_rating,
        round(rating / sum(rating) over (partition by country), 4) as rating_share
    from latest
    where rating <= 3.5
),
matched_keys as (
    -- Set difference on keys
    select country
    from enriched
    except distinct
    select country
    from suppliers
    where country = 'US'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join matched_keys k on s.country = k.country
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from verified s
    where exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.category in ('Fashion', 'Grocery', 'Books')
        )
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        cast(rating_share as int) as rating_share_int,
        least(prev_rating, rn) as least_prev_rating_rn,
        coalesce(city, 'N/A') as city_clean
    from eligible
    where city is not null
      and rating between 1.9 and 2.2
)
select
    id,
    country,
    city,
    rating,
    rn,
    prev_rating,
    rating_share,
    rating_share_int,
    least_prev_rating_rn,
    case when city rlike '^(Tokyo|Chiang Mai)$' then 'PRIORITY' when city rlike '^(Bangkok|New York)$' then 'EXTENDED' else 'OTHER_ZONE' end as city_zone
from valid
