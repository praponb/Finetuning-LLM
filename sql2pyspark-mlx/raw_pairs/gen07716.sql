with with_dims as (
    -- Attach products attributes
    select a.*, p.brand
    from inventory a
    inner join products p
        on a.product_id = p.id
),
cleaned as (
    -- Keep recent, valid rows
    select *, length(brand) as brand_length
    from with_dims
    where last_restock > date '2023-03-12'
      and brand is not null
),
cleaned_inventory as (
    -- Basic cleansing and defaults
    select
        *,
        upper(brand) as brand_upper,
        coalesce(brand, 'N/A') as resolved_brand
    from cleaned
    where brand is not null
      and brand_length < 53
),
enriched as (
    -- Attach stores attributes
    select
        src.*,
        s.store_name,
        s.sqft,
        s.region
    from cleaned_inventory src
    inner join stores s
        on src.store_id = s.id
),
enriched_inventory as (
    -- Attach products attributes
    select
        s.*,
        p.price,
        p.category
    from enriched s
    inner join products p
        on s.product_id = p.id
)
-- Multi-level aggregation
select
    date_trunc('month', last_restock) as activity_month,
    brand,
    count(*) as event_count,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    max(sqft) as max_sqft,
    count(*) as num_rows,
    round(sum(price) / nullif(count(case when category = 'Electronics' then 1 end), 0), 2) as price_per_electronics,
    dense_rank() over (partition by date_trunc('month', last_restock) order by max(sqft) desc) as max_sqft_dense_rank
from enriched_inventory
group by date_trunc('month', last_restock), brand
order by num_rows desc nulls first
