with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        product_id,
        last_restock,
        last_day(last_restock) as last_restock_month_end
    from inventory
    where quantity <= 180
),
enriched as (
    -- Ranking and gap features
    select
        id,
        product_id,
        row_number() over (partition by product_id order by last_restock_month_end, id) as row_seq,
        row_number() over (partition by product_id order by last_restock, id) as event_seq
    from cleaned
),
joined as (
    -- Attach products attributes
    select
        e.id,
        e.product_id,
        e.event_seq,
        p.launch_date,
        p.cost
    from enriched e
    left join products p
        on e.product_id = p.id
    where p.brand in ('Acme', 'Stark')
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        event_seq,
        launch_date,
        lag(cost) over (partition by product_id order by cost, id) as prev_cost,
        lead(cost) over (partition by product_id order by cost, id) as next_cost,
        lag(event_seq) over (partition by product_id order by cost, id) as prev_event_seq,
        row_number() over (partition by product_id order by cost, id) as row_seq,
        min(event_seq) over (partition by product_id) as product_id_min_event_seq
    from joined
),
metrics as (
    -- Ranking and gap features
    select
        id,
        event_seq,
        next_cost,
        prev_event_seq,
        row_seq,
        product_id_min_event_seq,
        min(event_seq) over (order by prev_event_seq, id rows between unbounded preceding and current row) as running_min_event_seq,
        avg(event_seq) over (order by prev_event_seq, id rows between unbounded preceding and current row) as running_avg_event_seq,
        count(event_seq) over (order by prev_event_seq, id rows between unbounded preceding and current row) as running_count_event_seq,
        last_value(row_seq, true) over (order by launch_date, id rows between unbounded preceding and current row) as last_known_row_seq
    from scored
),
scored_inventory as (
    -- Window metrics over several different partitions
    select
        *,
        sum(prev_event_seq) over (order by running_count_event_seq, id rows between 3 preceding and current row) as last4_sum_prev_event_seq,
        count(prev_event_seq) over (order by running_count_event_seq, id rows between 3 preceding and current row) as last4_count_prev_event_seq,
        min(prev_event_seq) over (order by running_count_event_seq, id rows between 3 preceding and current row) as last4_min_prev_event_seq,
        avg(prev_event_seq) over (order by running_count_event_seq, id rows between 3 preceding and current row) as last4_avg_prev_event_seq
    from metrics
),
metrics_inventory as (
    -- Window metrics over several different partitions
    select
        *,
        avg(last4_min_prev_event_seq) over (order by last4_sum_prev_event_seq, id rows between 2 preceding and 1 preceding) as last4_min_prev_event_seq_avg_2_prior,
        avg(product_id_min_event_seq) over (order by product_id_min_event_seq, id rows between 3 preceding and 1 preceding) as product_id_min_event_seq_avg_3_prior,
        first_value(last4_count_prev_event_seq, true) over (order by running_count_event_seq, id rows between unbounded preceding and current row) as first_known_last4_count_prev_event_seq
    from scored_inventory
),
grouped as (
    -- Multi-level aggregation
    select
        case when row_seq >= 7 then 'HIGH' else 'LOW' end as row_seq_tier,
        percentile_approx(last4_min_prev_event_seq, array(0.25, 0.5, 0.75)) as last4_min_prev_event_seq_quartiles,
        percentile_approx(event_seq, array(0.25, 0.5, 0.75)) as event_seq_quartiles
    from metrics_inventory
    group by 1
)
select row_seq_tier, last4_min_prev_event_seq_quartiles
from grouped;
