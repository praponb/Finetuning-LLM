with base as (
    -- Keep recent, valid rows
    select *, ceil(price) as price_whole
    from products
    where launch_date >= date '2023-09-05'
      and supplier_id is not null
),
joined as (
    -- Attach suppliers attributes
    select
        s.id,
        s.product_name,
        s.category,
        s.price,
        s.cost,
        s.launch_date,
        s.supplier_id,
        d.city,
        d.rating,
        d.country
    from base s
    inner join suppliers d
        on s.supplier_id = d.id
),
scored as (
    -- Ranking and gap features
    select
        id,
        product_name,
        category,
        launch_date,
        supplier_id,
        city,
        rating,
        ntile(3) over (order by cost, id) as cost_bucket,
        rank() over (partition by city order by rating desc nulls first) as rating_rank,
        corr(cost, price) over (partition by city) as corr_cost_price,
        lead(rating, 2) over (partition by supplier_id, country order by rating, id) as next_rating,
        sum(cost) over (partition by supplier_id order by launch_date, id rows between unbounded preceding and current row) as running_cost
    from joined
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_name,
        category,
        launch_date,
        supplier_id,
        city,
        rating,
        cost_bucket,
        rating_rank,
        corr_cost_price,
        next_rating,
        running_cost,
        next_rating / nullif(corr_cost_price, 0) as next_rating_per_corr_cost_price,
        coalesce(city, 'OTHER') as city_clean
    from scored
    where launch_date > date '2023-02-28'
      and supplier_id is not null
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        product_name,
        category,
        rating,
        cost_bucket,
        corr_cost_price
    from valid
    qualify row_number() over (partition by city order by launch_date, id) = 1
),
daily as (
    -- Grouped metrics
    select
        category,
        approx_percentile(rating, 0.9) as rating_p90,
        count(distinct category) as distinct_categories,
        size(collect_set(category)) as num_categories
    from deduped
    group by category
)
select
    category,
    distinct_categories,
    num_categories,
    case when category rlike '^(Books|Grocery)$' then 'GROUP_B' when category rlike '^(Electronics|Home)$' then 'GROUP_A' else 'PRIORITY' end as category_zone
from daily
order by category_zone;
