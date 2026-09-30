with enriched as (
    -- Ranking and gap features
    select
        id,
        product_id,
        unit_price,
        percent_rank() over (partition by product_id order by quantity desc) quantity_percent_rank,
        sum(quantity) over w_part running_quantity,
        ntile(10) over (partition by product_id order by unit_price desc, id) unit_price_bucket,
        corr(unit_price, quantity) over (partition by product_id, order_id) corr_unit_price_quantity,
        percent_rank() over (partition by order_id order by unit_price desc nulls last) unit_price_percent_rank
    from order_items
    window w_part as (partition by order_id order by quantity, id rows between unbounded preceding and current row)
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        floor(unit_price) unit_price_whole,
        round(running_quantity, 0) running_quantity_rounded
    from enriched
    where running_quantity = 17
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (order by unit_price desc nulls last) unit_price_dense_rank,
        sum(quantity_percent_rank) over (partition by product_id order by unit_price_bucket, id rows between unbounded preceding and current row) cumulative_quantity_percent_rank
    from filtered
),
scored as (
    -- Running and moving aggregates
    select
        *,
        max(quantity_percent_rank) over (partition by product_id) product_id_max_quantity_percent_rank,
        ntile(4) over (order by unit_price_bucket desc, id) unit_price_bucket_bucket
    from with_windows
    where unit_price >= 250
),
deduped as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by product_id order by unit_price_bucket_bucket desc nulls first) as dedup_rank
    from scored
    qualify dedup_rank = 1
),
with_dims as (
    -- Attach products attributes
    select
        s.id,
        s.product_id,
        s.quantity_percent_rank,
        s.running_quantity,
        s.unit_price_bucket,
        s.unit_price_percent_rank,
        s.unit_price_whole,
        s.running_quantity_rounded,
        s.unit_price_dense_rank,
        s.product_id_max_quantity_percent_rank,
        s.dedup_rank,
        d.cost
    from deduped s
    left join products d
        on s.product_id <=> d.id
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        quantity_percent_rank,
        running_quantity,
        unit_price_bucket,
        unit_price_percent_rank,
        unit_price_whole,
        running_quantity_rounded,
        unit_price_dense_rank,
        product_id_max_quantity_percent_rank,
        dedup_rank,
        cost,
        round(unit_price_dense_rank, 0) unit_price_dense_rank_rounded
    from with_dims
    where product_id is not null
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    metric,
    metric_value
from (
    select
        id,
        running_quantity_rounded,
        unit_price_dense_rank_rounded,
        cost
    from base
) src
unpivot (
    metric_value for metric in (running_quantity_rounded as RUNNING_QUANTITY_ROUNDED, unit_price_dense_rank_rounded as UNIT_PRICE_DENSE_RANK_ROUNDED, cost as COST_VAL)
)
