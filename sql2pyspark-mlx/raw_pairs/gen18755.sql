with valid as (
    select
        id,
        order_id,
        product_id,
        unit_price,
        round(unit_price * 0.9, 2) as unit_price_scaled,
        round(unit_price / quantity, 2) as line_total
    from order_items
    where product_id is not null
),
with_windows as (
    select
        id,
        order_id,
        product_id,
        line_total,
        avg(unit_price_scaled) over (partition by product_id order by line_total, id rows between 2 preceding and 1 preceding) as unit_price_scaled_avg_2_prior,
        lead(line_total, 2) over (partition by product_id order by line_total, id) as next_line_total,
        sum(unit_price_scaled) over (order by line_total, id rows between unbounded preceding and current row) as cumulative_unit_price_scaled,
        sum(line_total) over (partition by order_id order by unit_price, id rows between unbounded preceding and current row) as cumulative_line_total
    from valid
    where order_id in (15, 16, 36)
),
enriched as (
    select s.*, b.stock_qty
    from with_windows s
    join products b
        on s.product_id = b.id
),
with_windows_order_items as (
    select
        id,
        product_id,
        unit_price_scaled_avg_2_prior,
        cumulative_unit_price_scaled,
        avg(cumulative_line_total) over (partition by product_id, order_id order by next_line_total, id rows between 2 preceding and 1 preceding) as cumulative_line_total_avg_2_prior,
        ntile(10) over (partition by product_id order by cumulative_line_total desc, id) as cumulative_line_total_bucket,
        lag(next_line_total, 1, 0) over (order by cumulative_unit_price_scaled, id) as prev_next_line_total
    from enriched
),
metrics as (
    select
        id,
        cumulative_unit_price_scaled,
        cumulative_line_total_avg_2_prior,
        prev_next_line_total,
        sum(unit_price_scaled_avg_2_prior) over (partition by product_id order by cumulative_line_total_bucket, id rows between unbounded preceding and current row) as running_unit_price_scaled_avg_2_prior,
        dense_rank() over (partition by product_id order by cumulative_unit_price_scaled) as cumulative_unit_price_scaled_dense_rank
    from with_windows_order_items
)
select
    case when running_unit_price_scaled_avg_2_prior >= 389 then 'HIGH' else 'LOW' end as running_unit_price_scaled_avg_2_prior_tier,
    case when cumulative_line_total_avg_2_prior >= 415 then 'HIGH' else 'LOW' end as cumulative_line_total_avg_2_prior_tier,
    case when prev_next_line_total >= 79 then 'HIGH' else 'LOW' end as prev_next_line_total_tier,
    percentile_approx(cumulative_unit_price_scaled_dense_rank, 0.95) as cumulative_unit_price_scaled_dense_rank_p95,
    percentile_approx(cumulative_unit_price_scaled_dense_rank, array(0.25, 0.5, 0.75)) as cumulative_unit_price_scaled_dense_rank_quartiles,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2,
    grouping_id() as grouping_level
from metrics
group by grouping sets ((case when running_unit_price_scaled_avg_2_prior >= 389 then 'HIGH' else 'LOW' end, case when cumulative_line_total_avg_2_prior >= 415 then 'HIGH' else 'LOW' end, case when prev_next_line_total >= 79 then 'HIGH' else 'LOW' end), (case when running_unit_price_scaled_avg_2_prior >= 389 then 'HIGH' else 'LOW' end, case when cumulative_line_total_avg_2_prior >= 415 then 'HIGH' else 'LOW' end), ())
having distinct_ids_2 < 1000
order by running_unit_price_scaled_avg_2_prior_tier desc
