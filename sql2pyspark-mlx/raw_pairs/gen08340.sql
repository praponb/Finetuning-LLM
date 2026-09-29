with enriched as (
    -- Ranking and gap features
    select
        id,
        city,
        round(sqft / sum(sqft) over (partition by region), 4) as sqft_share,
        avg(sqft) over w_running as sqft_avg_3,
        round(sqft / sum(sqft) over (partition by region), 4) as sqft_share_2,
        ntile(4) over (order by sqft desc, id) as sqft_bucket,
        last_value(region, true) over (partition by date_trunc('week', opened_date), city order by opened_date, id rows between unbounded preceding and current row) as last_known_region
    from stores
    where store_name like 'M%'
    window w_running as (order by sqft, id rows between 3 preceding and current row)
),
with_latest as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select s.*, latest.sale_date
    from enriched s
    left join lateral (
            select c.sale_date
            from store_sales c
            where c.store_id = s.id
            order by c.sale_date desc, c.id desc
            limit 1
        ) latest on true
),
latest_match as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select s.*, latest.shipping_fee
    from with_latest s
    join lateral (
            select c.shipping_fee
            from orders c
            where c.store_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) latest on true
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by city order by sale_date desc, id) as row_num
    from latest_match
    qualify row_num = 1
),
with_latest_stores as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.quantity,
        top1.inventory_last_restock
    from latest s
    left join lateral (
            select c.quantity, c.last_restock as inventory_last_restock
            from inventory c
            where c.store_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        city,
        sqft_bucket,
        last_known_region,
        row_num,
        quantity,
        inventory_last_restock,
        row_number() over (partition by city order by row_num desc, id) as seq_no,
        lag(sqft_avg_3, 2) over (partition by city order by quantity, id) as prev_sqft_avg_3
    from with_latest_stores
)
-- Aggregate with conditional metrics
select
    case when quantity >= 214 then 'HIGH' else 'LOW' end as quantity_tier,
    case when seq_no >= 8 then 'HIGH' else 'LOW' end as seq_no_tier,
    count_if(city = 'Chiang Mai') as chiang_mai_events,
    count(case when city = 'Chiang Mai' then 1 end) as chiang_mai_count,
    sum(prev_sqft_avg_3) as total_prev_sqft_avg_3,
    size(collect_set(city)) as num_cities,
    count(distinct city) as distinct_cities,
    count_if(city = 'New York') as new_york_events
from scored
group by 1, 2
order by new_york_events desc;
