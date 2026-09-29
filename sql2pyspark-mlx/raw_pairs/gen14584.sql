with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        created_at,
        line.product_id,
        line.qty,
        line.promo
    from (
        select
            id,
            created_at,
            explode(items) as line
        from carts
    ) t
),
scored as (
    -- Running and moving aggregates
    select
        id,
        created_at,
        rank() over (order by qty nulls last) as qty_rank,
        max(product_id) over w_recent as created_at_to_date_max_product_id,
        avg(qty) over w_recent as created_at_to_date_avg_qty
    from exploded
    window w_recent as (partition by to_date(created_at), promo)
),
metrics as (
    -- Running and moving aggregates
    select
        qty_rank,
        created_at_to_date_max_product_id,
        created_at_to_date_avg_qty,
        sum(qty_rank) over (order by cast(created_at as bigint) range between 900 preceding and current row) as qty_rank_sum_15m,
        avg(created_at_to_date_max_product_id) over (order by cast(created_at as bigint) range between 3600 preceding and current row) as created_at_to_date_max_product_id_avg_1h,
        percent_rank() over (order by created_at_to_date_max_product_id desc nulls first) as created_at_to_date_max_product_id_percent_rank,
        rank() over (order by qty_rank desc) as qty_rank_rank
    from scored
),
scored_carts as (
    -- Ranking and gap features
    select
        *,
        rank() over (order by created_at_to_date_avg_qty) as created_at_to_date_avg_qty_rank,
        percent_rank() over (order by created_at_to_date_max_product_id_avg_1h desc nulls first) as created_at_to_date_max_product_id_avg_1h_percent_rank
    from metrics
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        created_at_to_date_avg_qty,
        qty_rank_sum_15m,
        created_at_to_date_max_product_id_avg_1h,
        qty_rank_rank,
        created_at_to_date_avg_qty_rank,
        created_at_to_date_max_product_id_avg_1h_percent_rank,
        created_at_to_date_avg_qty_rank * created_at_to_date_max_product_id as created_at_to_date_avg_qty_rank_times_created_at_to_date_max_product_id,
        round(qty_rank_sum_15m / created_at_to_date_max_product_id, 2) as qty_rank_sum_15m_per_created_at_to_date_max_product_id
    from scored_carts
    where created_at_to_date_avg_qty_rank > 8
)
select
    created_at_to_date_avg_qty,
    qty_rank_sum_15m,
    created_at_to_date_max_product_id_avg_1h,
    created_at_to_date_avg_qty_rank,
    qty_rank_sum_15m_per_created_at_to_date_max_product_id
from valid;
