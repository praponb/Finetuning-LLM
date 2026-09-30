with valid as (
    -- Basic cleansing and defaults
    select
        *,
        round(revenue, 1) as revenue_rounded,
        round(revenue, 1) as revenue_rounded_2
    from store_sales
    where sale_date > date '2023-03-03'
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by product_id order by revenue_rounded, id) as latest_rank
    from valid
    qualify latest_rank = 1
),
enriched as (
    -- Attach products attributes
    select
        s.*,
        p.category,
        p.product_name,
        p.price
    from latest s
    left join products p
        on s.product_id = p.id
),
joined as (
    -- Attach stores attributes
    select
        src.*,
        s.sqft,
        s.store_name,
        s.city
    from enriched src
    left join stores s
        on src.store_id = s.id
    where s.store_name like 'Si%'
),
scored as (
    -- Ranking and gap features
    select
        id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_rounded_2,
        price,
        store_name,
        city,
        avg(sqft) over (partition by city, product_id order by revenue_rounded_2, id rows between 5 preceding and 1 preceding) as sqft_avg_5_prior,
        corr(revenue, units) over (partition by year(sale_date), store_id) as corr_revenue_units
    from joined
)
select
    id,
    sale_date,
    revenue,
    revenue_rounded_2,
    price,
    store_name
from scored
