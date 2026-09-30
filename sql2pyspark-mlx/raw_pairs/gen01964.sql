with enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(revenue) over (partition by store_id order by revenue asc, id asc rows between 2 preceding and current row) as last3_sum_revenue,
        min(revenue) over (partition by store_id order by revenue asc, id asc rows between 2 preceding and current row) as last3_min_revenue,
        avg(revenue) over (partition by store_id order by revenue asc, id asc rows between 2 preceding and current row) as last3_avg_revenue
    from store_sales
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        sum(revenue) over (partition by product_id order by last3_min_revenue asc, id asc rows between unbounded preceding and current row) as running_sum_revenue,
        max(revenue) over (partition by product_id order by last3_min_revenue asc, id asc rows between unbounded preceding and current row) as running_max_revenue,
        min(revenue) over (partition by product_id order by last3_min_revenue asc, id asc rows between unbounded preceding and current row) as running_min_revenue
    from enriched
),
enriched_store_sales as (
    -- Ranking and gap features
    select
        id,
        store_id,
        product_id,
        units,
        last3_avg_revenue,
        running_max_revenue,
        running_min_revenue,
        lag(running_max_revenue) over (partition by product_id order by revenue asc, id asc) as prev_running_max_revenue,
        lead(running_max_revenue) over (partition by product_id order by revenue asc, id asc) as next_running_max_revenue,
        lead(running_sum_revenue) over (partition by product_id order by revenue asc, id asc) as next_running_sum_revenue,
        lag(running_sum_revenue) over (partition by product_id order by revenue asc, id asc) as prev_running_sum_revenue,
        row_number() over (partition by product_id order by revenue asc, id asc) as row_seq,
        avg(running_max_revenue) over (partition by product_id order by running_min_revenue asc, id asc rows between 5 preceding and current row) as running_max_revenue_avg_5
    from metrics
    where last3_sum_revenue >= 19000
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from enriched_store_sales
    qualify row_number() over (partition by product_id order by running_max_revenue_avg_5 desc nulls last, id asc) = 1
),
metrics_store_sales as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        units,
        last3_avg_revenue,
        running_max_revenue,
        prev_running_max_revenue,
        next_running_max_revenue,
        next_running_sum_revenue,
        prev_running_sum_revenue,
        avg(prev_running_sum_revenue) over w_hist as running_avg_prev_running_sum_revenue,
        max(prev_running_sum_revenue) over w_hist as running_max_prev_running_sum_revenue,
        sum(prev_running_sum_revenue) over w_hist as running_sum_prev_running_sum_revenue,
        ntile(5) over w_part as next_running_sum_revenue_bucket
    from deduped
    window w_hist as (partition by store_id order by prev_running_sum_revenue asc, id asc rows between unbounded preceding and current row), w_part as (order by next_running_sum_revenue asc, id asc)
),
enriched_store_sales_2 as (
    -- Ranking and gap features
    select
        *,
        sum(running_sum_prev_running_sum_revenue) over (order by next_running_sum_revenue_bucket asc, id asc rows between unbounded preceding and current row) as cumulative_running_sum_prev_running_sum_revenue,
        round(last3_avg_revenue / sum(last3_avg_revenue) over (partition by store_id), 4) as last3_avg_revenue_share,
        avg(next_running_max_revenue) over (partition by store_id order by prev_running_sum_revenue asc, id asc rows between 2 preceding and 1 preceding) as next_running_max_revenue_avg_2_prior,
        avg(prev_running_sum_revenue) over (order by next_running_sum_revenue asc, id asc rows between 3 preceding and 1 preceding) as prev_running_sum_revenue_avg_3_prior,
        row_number() over (order by next_running_max_revenue asc, id asc) as event_seq
    from metrics_store_sales
),
deduped_store_sales as (
    -- QUALIFY-style window filter
    select *
    from enriched_store_sales_2
    qualify row_number() over (partition by store_id order by next_running_sum_revenue desc, id asc) = 1
)
select
    id,
    units,
    running_max_revenue,
    prev_running_max_revenue,
    next_running_max_revenue,
    next_running_sum_revenue,
    prev_running_sum_revenue,
    last3_avg_revenue_share,
    prev_running_sum_revenue_avg_3_prior
from deduped_store_sales
order by prev_running_max_revenue desc;
