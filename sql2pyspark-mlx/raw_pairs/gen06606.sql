with enriched as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by carrier order by cost, id) as row_seq,
        round(cost / sum(cost) over (partition by order_id, status), 4) as cost_share
    from shipments
    where shipped_date > '2024-11-01'
),
with_dims as (
    -- Attach orders attributes
    select
        s.*,
        p.discount,
        p.status as order_status,
        p.order_date
    from enriched s
    inner join orders p
        on s.order_id <=> p.id
),
enriched_shipments as (
    -- Attach orders attributes
    select s.*, d.order_date as order_order_date
    from with_dims s
    inner join orders d
        on s.order_id = d.id
),
metrics as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over (order by row_seq nulls last) as row_seq_cume_dist,
        last_value(cost) ignore nulls over (partition by order_status, status order by delivered_date, id rows between unbounded preceding and current row) as last_known_cost
    from enriched_shipments
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        order_date,
        row_seq_cume_dist,
        avg(row_seq_cume_dist) over (partition by order_status order by row_seq, id rows between 5 preceding and current row) as row_seq_cume_dist_avg_5,
        ntile(10) over (partition by order_id order by last_known_cost desc, id) as last_known_cost_bucket
    from metrics
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by order_id, status order by cost desc, id) as rank_in_group
    from scored
    qualify rank_in_group = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        shipped_date,
        cost,
        row_seq_cume_dist,
        last_known_cost_bucket,
        rank_in_group,
        ntile(4) over (partition by date_trunc('week', shipped_date), order_id order by row_seq_cume_dist_avg_5, id) as row_seq_cume_dist_avg_5_bucket,
        avg(row_seq_cume_dist_avg_5) over (partition by date_trunc('month', order_date), status) as order_date_period_avg_row_seq_cume_dist_avg_5
    from ranked
)
-- Grouped metrics
select
    order_id,
    max(order_date_period_avg_row_seq_cume_dist_avg_5) as max_order_date_period_avg_row_seq_cume_dist_avg_5,
    count(distinct id) as distinct_ids,
    count(distinct order_id) as distinct_order_ids,
    round(avg(cost), 2) as avg_cost,
    approx_percentile(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
    round(avg(last_known_cost_bucket), 2) as avg_last_known_cost_bucket
from with_windows
group by 1
