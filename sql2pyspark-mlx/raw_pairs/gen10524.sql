with filtered as (
    -- Keep recent, valid rows
    select
        *,
        initcap(trim(brand)) as brand_title,
        round(stock_qty * 0.85, 2) as stock_qty_scaled,
        coalesce(brand, 'OTHER') as resolved_brand
    from products
    where brand is not null
      and category <> 'Grocery'
),
joined as (
    -- Attach suppliers attributes
    select
        e.id,
        e.product_name,
        e.category,
        e.brand,
        e.price,
        e.cost,
        e.launch_date,
        e.supplier_id,
        e.brand_title,
        e.stock_qty_scaled,
        b.city,
        b.rating,
        b.supplier_name
    from filtered e
    left join suppliers b
        on e.supplier_id <=> b.id
    where b.rating <= 3.9
),
matched_keys as (
    -- Set difference on keys
    select brand, id
    from joined
    except
    select brand, id
    from products
    where supplier_id in (1, 11, 28)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    inner join matched_keys k on s.brand = k.brand and s.id = k.id
),
joined_products as (
    -- Attach suppliers attributes
    select
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.price,
        s.cost,
        s.supplier_id,
        s.city,
        s.rating,
        p2.supplier_name as supplier_supplier_name,
        p2.city as supplier_city
    from verified s
    left join suppliers p2
        on s.supplier_id = p2.id
),
filtered_products as (
    -- Basic cleansing and defaults
    select
        *,
        floor(rating) as rating_whole,
        coalesce(city, 'DIRECT') as city_filled
    from joined_products
    where cost >= 270
),
grouped as (
    -- Aggregate with conditional metrics
    select
        case when cost >= 720 then 'HIGH' else 'LOW' end as cost_tier,
        case when rating_whole >= 81 then 'PREMIUM' else 'STANDARD' end as rating_whole_tier,
        count(*) filter (where category = 'Fashion') as fashion_rows,
        sum(case when category = 'Books' then rating else 0 end) as books_rating,
        approx_percentile(price, array(0.25, 0.5, 0.75)) as price_quartiles,
        stddev_samp(price) as price_stddev,
        grouping(case when cost >= 720 then 'HIGH' else 'LOW' end) as grouping_id
    from filtered_products
    group by rollup (case when cost >= 720 then 'HIGH' else 'LOW' end, case when rating_whole >= 81 then 'PREMIUM' else 'STANDARD' end)
    having sum(case when category = 'Books' then rating else 0 end) > 0
)
select
    fashion_rows,
    books_rating,
    price_stddev,
    grouping_id
from grouped
order by grouping_id desc
