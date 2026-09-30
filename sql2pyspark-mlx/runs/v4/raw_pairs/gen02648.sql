with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        initcap(trim(country)) as country_title,
        round(rating * 0.85, 2) as rating_scaled
    from suppliers
    where city is not null
      and supplier_name like 'Li%'
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.price < 580
        )
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.product_category,
        x.stock_qty
    from not_flagged s
    left join lateral (
            select c.category as product_category, c.stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) x on true
),
summary as (
    -- Grouped metrics
    select
        case when stock_qty >= 440 then 'LARGE' else 'SMALL' end as stock_qty_tier,
        case when rating >= 4.3 then 'PREMIUM' else 'STANDARD' end as rating_tier,
        count(*) as cnt,
        round(avg(rating_scaled), 2) as avg_rating_scaled,
        count_if(country = 'JP') as jp_events,
        rank() over (partition by case when stock_qty >= 440 then 'LARGE' else 'SMALL' end order by count(*) desc) as cnt_rank
    from latest_match
    group by 1, 2
)
select
    stock_qty_tier,
    rating_tier,
    jp_events,
    cnt_rank
from summary
order by jp_events nulls last
