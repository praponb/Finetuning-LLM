with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price + quantity as line_total,
        unit_price - quantity as line_total_2
    from order_items
    where unit_price between 200 and 350
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        order_id,
        product_id,
        unit_price,
        line_total_2,
        lead(quantity, 2) over (partition by product_id, order_id order by quantity, id) as next_quantity,
        lag(quantity, 1) over (partition by product_id, order_id order by line_total_2, id) as prev_quantity,
        round(unit_price / sum(unit_price) over (partition by order_id), 4) as unit_price_share
    from cleaned
    where quantity != 4
),
matched_keys as (
    -- Set difference on keys
    select order_id
    from metrics
    except
    select order_id
    from order_items
    where unit_price < 400
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join matched_keys k on s.order_id = k.order_id
),
with_windows as (
    -- Ranking and gap features
    select
        order_id,
        line_total_2,
        next_quantity,
        prev_quantity,
        percent_rank() over (partition by product_id order by unit_price desc nulls last) as unit_price_percent_rank,
        min(prev_quantity) over (partition by product_id) as product_id_min_prev_quantity,
        rank() over (order by prev_quantity nulls last) as prev_quantity_rank
    from verified
),
scored as (
    -- Running and moving aggregates
    select *, cume_dist() over (partition by order_id order by product_id_min_prev_quantity nulls first) as product_id_min_prev_quantity_cume_dist
    from with_windows
),
stats as (
    -- Grouped metrics
    select
        case when prev_quantity_rank >= 8 then 'PREMIUM' else 'STANDARD' end as prev_quantity_rank_tier,
        case when product_id_min_prev_quantity >= 7 then 'HIGH' else 'LOW' end as product_id_min_prev_quantity_tier,
        percentile_approx(prev_quantity, 0.5) as prev_quantity_p50,
        percentile_approx(line_total_2, array(0.25, 0.5, 0.75)) as line_total_2_quartiles,
        count(*) as row_count,
        avg(prev_quantity_rank) as avg_prev_quantity_rank
    from scored
    group by case when prev_quantity_rank >= 8 then 'PREMIUM' else 'STANDARD' end, case when product_id_min_prev_quantity >= 7 then 'HIGH' else 'LOW' end
)
select
    prev_quantity_rank_tier,
    product_id_min_prev_quantity_tier,
    prev_quantity_p50
from stats
order by prev_quantity_rank_tier desc nulls first;
