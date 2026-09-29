with base as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        city,
        rating,
        substring(supplier_name, 1, 4) as supplier_name_prefix,
        coalesce(city, 'NONE') as city_clean
    from suppliers
    where rating <= 3.6
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.rating,
        top1.product_launch_date
    from base s
    left join lateral (
            select c.launch_date as product_launch_date
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) top1 on true
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_last s
    where exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.launch_date <= s.product_launch_date
        )
),
kept_keys as (
    -- Set intersection on keys
    select id
    from kept
    intersect
    select id
    from suppliers
    where country = 'UK'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept s
    join kept_keys k on s.id = k.id
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        case when city in ('London', 'Singapore') then 'Priority' else 'Other' end as city_group,
        cast(rating as int) as rating_int,
        coalesce(city, 'OTHER') as resolved_city
    from kept_rows
    where rating < 3.6
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        max(rating_int) over (partition by city) as city_max_rating_int,
        stddev_samp(rating) over (partition by city) as city_stddev_samp_rating,
        sum(rating) over (partition by month(product_launch_date), city) as product_launch_date_month_sum_rating
    from filtered
    where product_launch_date between '2024-05-13' and '2025-09-08'
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        rating,
        rating_int,
        resolved_city,
        city_max_rating_int,
        city_stddev_samp_rating,
        round(city_stddev_samp_rating / sum(city_stddev_samp_rating) over (partition by date_trunc('week', product_launch_date)), 4) as city_stddev_samp_rating_share,
        percent_rank() over (order by rating_int desc nulls last) as rating_int_percent_rank,
        sum(rating) over (partition by city) as city_sum_rating
    from with_windows
)
select
    rating,
    rating_int,
    city_max_rating_int,
    city_stddev_samp_rating,
    city_stddev_samp_rating_share,
    rating_int_percent_rank
from enriched;
