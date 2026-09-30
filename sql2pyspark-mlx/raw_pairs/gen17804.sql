with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        round(rating, 1) as rating_rounded,
        case when rating >= 3.6 then 'High' when rating >= 1.5 then 'Medium' else 'Low' end as rating_band,
        coalesce(city, 'DIRECT') as city_filled
    from suppliers
    where city is not null
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.brand <> 'Globex'
        )
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by city order by rating desc, id) as latest_rank
        from eligible
    ) x
    where latest_rank = 1
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.supplier_name,
        s.country,
        s.city,
        s.rating,
        s.rating_rounded,
        s.rating_band,
        s.city_filled,
        x.category,
        x.product_stock_qty
    from latest s
    join lateral (
            select c.category, c.stock_qty as product_stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) x on true
),
scored as (
    -- Ranking and gap features
    select
        id,
        supplier_name,
        rating,
        city_filled,
        category,
        row_number() over w_recent as event_seq,
        first_value(city) ignore nulls over (partition by country order by rating_rounded, id rows between unbounded preceding and current row) as first_known_city,
        lag(rating, 2, 0) over (partition by city order by product_stock_qty, id) as prev_rating
    from with_last
    where supplier_name is not null
    window w_recent as (partition by country order by rating_rounded, id)
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        sum(rating) over (order by rating, id rows between unbounded preceding and current row) as cumulative_rating,
        lead(rating, 1) over (partition by category order by prev_rating, id) as next_rating
    from scored
)
select
    id,
    supplier_name,
    rating,
    city_filled,
    event_seq,
    prev_rating,
    cumulative_rating,
    case when supplier_name rlike '^(EuroGoods GmbH|Global Supply)$' then 'CORE' when supplier_name rlike '^(Siam Parts Co|Tokyo Traders)$' then 'GROUP_B' else 'PRIORITY' end as supplier_name_zone
from metrics
order by prev_rating desc nulls first;
