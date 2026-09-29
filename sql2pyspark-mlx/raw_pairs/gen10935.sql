with latest_match as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select s.*, top1.quantity
    from products s
    join lateral (
            select c.quantity
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
enriched as (
    -- Attach suppliers attributes
    select
        s.*,
        p.supplier_name,
        p.rating,
        p.city
    from latest_match s
    inner join suppliers p
        on s.supplier_id <=> p.id
),
metrics as (
    -- Ranking and gap features
    select
        id,
        product_name,
        category,
        price,
        stock_qty,
        launch_date,
        quantity,
        supplier_name,
        rating,
        city,
        min(rating) over (partition by supplier_id order by rating, id rows between 3 preceding and current row) as last4_min_rating,
        sum(rating) over (partition by supplier_id order by rating, id rows between 3 preceding and current row) as last4_sum_rating,
        avg(rating) over (partition by supplier_id order by rating, id rows between 3 preceding and current row) as last4_avg_rating
    from enriched
),
with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.order_item_unit_price,
        x.order_item_quantity
    from metrics s
    left join lateral (
            select c.unit_price as order_item_unit_price, c.quantity as order_item_quantity
            from order_items c
            where c.product_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) x on true
),
totals as (
    -- Grouped metrics
    select
        city,
        case when last4_sum_rating >= 20.3 then 'HIGH' else 'LOW' end as last4_sum_rating_tier,
        date_trunc('week', launch_date) as week_start,
        count(case when city = 'Singapore' then 1 end) as singapore_count,
        count(*) as cnt
    from with_latest
    group by 1, 2, 3
)
select
    city,
    last4_sum_rating_tier,
    week_start,
    singapore_count
from totals
order by week_start desc nulls last, singapore_count;
