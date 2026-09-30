with scored as (
    -- Ranking and gap features
    select
        id,
        supplier_name,
        rating,
        avg(rating) over (partition by country order by rating, id rows between 6 preceding and current row) as rating_avg_6,
        sum(rating) over (partition by city) as city_sum_rating
    from suppliers
    where city != 'Chiang Mai'
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.cost <= 540
        )
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.product_cost,
        latest.product_category
    from kept s
    left join lateral (
            select c.cost as product_cost, c.category as product_category
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) latest on true
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.rating_avg_6,
        s.city_sum_rating,
        s.product_category,
        x.brand,
        x.product_launch_date
    from latest_match s
    left join lateral (
            select c.brand, c.launch_date as product_launch_date
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) x on true
),
kept_suppliers as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_last s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.launch_date < s.product_launch_date
        )
),
totals as (
    -- Multi-level aggregation
    select
        product_category,
        brand,
        size(collect_set(product_category)) as num_product_categories,
        avg(city_sum_rating) as avg_city_sum_rating,
        sum(case when brand = 'Acme' then city_sum_rating else 0 end) as acme_city_sum_rating,
        count(*) filter (where product_category = 'Grocery') as grocery_rows,
        max(city_sum_rating) as max_city_sum_rating,
        max(rating_avg_6) filter (where product_category = 'Sports') as sports_max_rating_avg_6
    from kept_suppliers
    group by product_category, brand
)
select
    product_category,
    brand,
    num_product_categories,
    acme_city_sum_rating,
    grocery_rows,
    max_city_sum_rating,
    sports_max_rating_avg_6,
    case when product_category rlike '^(Home|Books)$' then 'OTHER_ZONE' when product_category rlike '^(Electronics|Sports)$' then 'CORE' else 'EXTENDED' end as product_category_zone
from totals;
