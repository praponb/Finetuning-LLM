with filtered as (
    -- Keep recent, valid rows
    select *, initcap(trim(country)) as country_title
    from suppliers
    where city is not null
      and rating is not null
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.supplier_name,
        latest.product_price,
        latest.product_category
    from filtered s
    left join lateral (
            select c.price as product_price, c.category as product_category
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) latest on true
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by product_category order by product_price asc) as rank_in_group
    from with_latest
    qualify rank_in_group = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        lead(product_price) over (partition by product_category order by rank_in_group asc, id asc) as next_product_price,
        lag(product_price) over (partition by product_category order by rank_in_group asc, id asc) as prev_product_price,
        row_number() over (partition by product_category order by rank_in_group asc, id asc) as row_seq
    from ranked
)
-- Multi-level aggregation
select
    case when product_price >= 1220 then 'PREMIUM' else 'STANDARD' end as product_price_tier,
    round(avg(row_seq), 2) as avg_row_seq,
    count(*) as row_count,
    size(collect_set(product_category)) as num_product_categories
from metrics
group by case when product_price >= 1220 then 'PREMIUM' else 'STANDARD' end
order by row_count desc;
