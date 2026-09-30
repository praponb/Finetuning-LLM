with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        country,
        city,
        rating,
        trim(country) clean_country,
        lower(country) country_lower,
        coalesce(city, 'UNKNOWN') city_clean
    from suppliers
    where city is distinct from 'Chiang Mai'
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.stock_qty != 261
        )
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.product_category,
        x.brand
    from eligible s
    join lateral (
            select c.category as product_category, c.brand
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) x on true
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        country,
        city,
        rating,
        ntile(3) over (partition by product_category order by rating desc, id) rating_bucket,
        avg(rating) over (order by rating, id rows between 5 preceding and 1 preceding) rating_avg_5_prior,
        first_value(product_category) ignore nulls over (partition by brand order by rating, id rows between unbounded preceding and current row) first_known_product_category
    from with_latest
    where product_category = 'Electronics'
),
matched_keys as (
    -- Set difference on keys
    select country, city
    from enriched
    except distinct
    select country, city
    from suppliers
    where city in ('Singapore', 'Bangkok', 'Tokyo')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    join matched_keys k on s.country = k.country and s.city = k.city
)
select
    id,
    country,
    city,
    rating,
    rating_bucket,
    rating_avg_5_prior,
    first_known_product_category,
    case when country rlike '^(US|JP)$' then 'CORE' else 'OTHER_ZONE' end as country_zone
from verified
order by country_zone;
