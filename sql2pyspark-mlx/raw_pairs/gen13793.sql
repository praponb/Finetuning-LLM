with latest as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by order_id order by delivered_date asc nulls last) as rank_in_group
    from shipments
    qualify rank_in_group = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        cost,
        status,
        ntile(5) over (partition by order_id order by cost asc, id asc) as cost_bucket,
        round(cost / sum(cost) over (partition by order_id), 4) as cost_share,
        sum(cost) over (partition by carrier order by shipped_date asc, id asc rows between unbounded preceding and current row) as running_cost,
        avg(cost) over (partition by status order by cost asc, id asc rows between 3 preceding and 1 preceding) as cost_avg_3_prior
    from latest
),
metrics_shipments as (
    -- Window metrics over several different partitions
    select
        *,
        avg(cost_avg_3_prior) over (partition by status) as status_avg_cost_avg_3_prior,
        avg(cost_avg_3_prior) over (order by running_cost asc, id asc rows between 2 preceding and 1 preceding) as cost_avg_3_prior_avg_2_prior,
        last_value(cost_share) ignore nulls over (partition by status order by cost_avg_3_prior asc, id asc rows between unbounded preceding and current row) as last_known_cost_share,
        row_number() over (partition by status order by cost_bucket asc, id asc) as event_seq
    from metrics
),
scored as (
    -- Ranking and gap features
    select
        id,
        status,
        cost_bucket,
        cost_share,
        running_cost,
        cost_avg_3_prior_avg_2_prior,
        last_known_cost_share,
        max(event_seq) over (partition by status) as status_max_event_seq,
        lag(cost_avg_3_prior, 1) over (order by running_cost asc, id asc) as prev_cost_avg_3_prior,
        cume_dist() over (order by status_avg_cost_avg_3_prior desc nulls last) as status_avg_cost_avg_3_prior_cume_dist,
        sum(cost_avg_3_prior) over (partition by status order by status_avg_cost_avg_3_prior asc, id asc rows between unbounded preceding and current row) as cumulative_cost_avg_3_prior,
        sum(status_avg_cost_avg_3_prior) over (partition by status order by cost asc, id asc rows between unbounded preceding and current row) as running_status_avg_cost_avg_3_prior
    from metrics_shipments
)
-- Aggregate with conditional metrics
select
    case when cost_avg_3_prior_avg_2_prior >= 97 then 'PREMIUM' else 'STANDARD' end as cost_avg_3_prior_avg_2_prior_tier,
    status,
    case when last_known_cost_share >= 0.71 then 'HIGH' else 'LOW' end as last_known_cost_share_tier,
    sort_array(collect_set(status)) as status_list,
    size(collect_set(status)) as num_status,
    approx_percentile(last_known_cost_share, array(0.25, 0.5, 0.75)) as last_known_cost_share_quartiles
from scored
group by cost_avg_3_prior_avg_2_prior_tier, status, last_known_cost_share_tier
having num_status > 1
order by status desc
