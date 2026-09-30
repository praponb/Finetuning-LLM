with ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by store_id order by sale_date asc, id asc) as dedup_rank
        from store_sales
    ) t
    where dedup_rank = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        sale_date,
        units,
        dedup_rank,
        round(revenue / sum(revenue) over (partition by product_id), 4) revenue_share,
        corr(units, revenue) over (partition by product_id, store_id) corr_units_revenue,
        avg(revenue) over (order by dedup_rank asc, id asc rows between 2 preceding and 1 preceding) revenue_avg_2_prior,
        last_value(units, true) over (partition by product_id, store_id order by dedup_rank asc, id asc rows between unbounded preceding and current row) last_known_units,
        rank() over (partition by product_id order by revenue desc nulls last) revenue_rank
    from ranked
),
metrics_store_sales as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (order by corr_units_revenue desc, id asc) row_seq,
        ntile(5) over (order by revenue_share desc, id asc) revenue_share_bucket,
        ntile(5) over (order by revenue_share asc, id asc) revenue_share_bucket_2,
        sum(revenue_rank) over (order by last_known_units asc, id asc rows between unbounded preceding and current row) running_revenue_rank,
        percent_rank() over (order by corr_units_revenue desc nulls last) corr_units_revenue_percent_rank
    from metrics
    where last_known_units = 24
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        sum(revenue_share_bucket_2) over w_grp running_revenue_share_bucket_2,
        last_value(row_seq) ignore nulls over (order by revenue_rank asc, id asc rows between unbounded preceding and current row) last_known_row_seq,
        lead(revenue_share_bucket, 1) over w_grp_2 next_revenue_share_bucket,
        ntile(5) over (order by row_seq desc, id asc) row_seq_bucket,
        row_number() over (order by last_known_units asc, id asc) seq_no
    from metrics_store_sales
    window w_grp as (order by dedup_rank asc, id asc rows between unbounded preceding and current row), w_grp_2 as (order by revenue_rank asc, id asc)
),
totals as (
    -- Aggregate with conditional metrics
    select
        seq_no,
        case when next_revenue_share_bucket >= 8 then 'PREMIUM' else 'STANDARD' end as next_revenue_share_bucket_tier,
        approx_percentile(last_known_units, 0.5) as last_known_units_p50,
        percentile_approx(revenue_share_bucket, 0.9) as revenue_share_bucket_p90,
        approx_percentile(revenue_share, 0.9) as revenue_share_p90,
        sum(running_revenue_share_bucket_2) as total_running_revenue_share_bucket_2
    from scored
    group by 1, 2
)
select
    seq_no,
    next_revenue_share_bucket_tier,
    last_known_units_p50,
    revenue_share_p90
from totals;
