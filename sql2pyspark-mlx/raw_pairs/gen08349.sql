with enriched as (
    -- Ranking and gap features
    select
        id,
        order_id,
        shipped_date,
        cost,
        avg(cost) over (order by shipped_date asc, id asc rows between 2 preceding and 1 preceding) as cost_avg_2_prior,
        ntile(3) over (partition by date_trunc('month', delivered_date) order by cost asc, id asc) as cost_bucket,
        row_number() over (order by cost asc, id asc) as event_seq,
        stddev_samp(cost) over (partition by status) as status_stddev_samp_cost
    from shipments
    where carrier != 'Kerry'
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        order_id,
        cost,
        cost_bucket,
        status_stddev_samp_cost
    from enriched
    qualify row_number() over (partition by order_id order by cost_avg_2_prior asc nulls last, id asc) = 1
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from latest
    qualify row_number() over (partition by order_id order by cost desc nulls last, id asc) = 1
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        cost,
        cost_bucket,
        status_stddev_samp_cost,
        round(status_stddev_samp_cost / cost, 2) as status_stddev_samp_cost_per_cost
    from ranked
    where order_id = 30
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        min(cost_bucket) over w_hist as running_min_cost_bucket,
        max(cost_bucket) over w_hist as running_max_cost_bucket,
        count(cost_bucket) over w_hist as running_count_cost_bucket,
        avg(cost_bucket) over w_hist as running_avg_cost_bucket,
        last_value(cost, true) over (order by cost_bucket asc, id asc rows between unbounded preceding and current row) as last_known_cost
    from filtered
    window w_hist as (partition by order_id order by cost_bucket asc, id asc rows between unbounded preceding and current row)
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    order_id,
    measure,
    metric_value
from (
    select
        id,
        order_id,
        last_known_cost,
        cost
    from metrics
) src
unpivot include nulls (
    metric_value for measure in (last_known_cost, cost)
) u
