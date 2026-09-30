with valid as (
    -- Basic cleansing and defaults
    select
        *,
        length(carrier) carrier_length,
        coalesce(carrier, 'NONE') carrier_filled
    from shipments
    where carrier is not null
),
with_dims as (
    -- Attach orders attributes
    select
        s.*,
        b.channel,
        b.amount,
        b.order_date
    from valid s
    join orders b
        on s.order_id = b.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(cost) ignore nulls over w_recent first_known_cost,
        ntile(3) over w_grp amount_bucket,
        sum(carrier_length) over w_recent cumulative_carrier_length
    from with_dims
    window w_recent as (order by amount, id rows between unbounded preceding and current row), w_grp as (partition by order_id, status order by amount desc, id)
),
scored as (
    -- Running and moving aggregates
    select
        *,
        max(carrier_length) over (partition by order_id) order_id_max_carrier_length,
        row_number() over (partition by status order by shipped_date, id) seq_no,
        percent_rank() over (partition by order_id, carrier order by cost desc nulls last) cost_percent_rank,
        row_number() over (partition by carrier order by amount desc, id) seq_no_2
    from with_windows
),
scored_shipments as (
    -- Ranking and gap features
    select
        *,
        max(amount_bucket) over w_hist carrier_max_amount_bucket,
        ntile(5) over (partition by carrier order by order_id_max_carrier_length, id) order_id_max_carrier_length_bucket,
        row_number() over (partition by order_id order by cumulative_carrier_length, id) event_seq,
        first_value(channel) ignore nulls over w_ordered first_known_channel,
        first_value(first_known_cost, true) over w_running first_known_first_known_cost
    from scored
    window w_hist as (partition by carrier), w_ordered as (partition by channel order by order_date, id rows between unbounded preceding and current row), w_running as (partition by channel order by amount, id rows between unbounded preceding and current row)
),
with_windows_shipments as (
    -- Ranking and gap features
    select
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_length,
        carrier_filled,
        channel,
        amount,
        order_date,
        first_known_cost,
        amount_bucket,
        cumulative_carrier_length,
        order_id_max_carrier_length,
        seq_no,
        cost_percent_rank,
        seq_no_2,
        carrier_max_amount_bucket,
        order_id_max_carrier_length_bucket,
        event_seq,
        first_known_channel,
        first_known_first_known_cost,
        max(carrier_length) over (partition by carrier, order_id) carrier_max_carrier_length,
        row_number() over (order by carrier_max_amount_bucket desc, id) event_seq_2
    from scored_shipments
),
scored_shipments_2 as (
    -- Ranking and gap features
    select
        *,
        round(seq_no_2 / sum(seq_no_2) over (partition by order_id), 4) seq_no_2_share,
        last_value(cost_percent_rank) ignore nulls over (partition by carrier order by cost_percent_rank, id rows between unbounded preceding and current row) last_known_cost_percent_rank,
        lag(cumulative_carrier_length, 2) over (partition by channel order by seq_no_2, id) prev_cumulative_carrier_length
    from with_windows_shipments
)
-- Aggregate with conditional metrics
select
    case when seq_no_2 >= 8 then 'PREMIUM' else 'STANDARD' end as seq_no_2_tier,
    case when first_known_first_known_cost >= 101 then 'PREMIUM' else 'STANDARD' end as first_known_first_known_cost_tier,
    round(sum(last_known_cost_percent_rank) / nullif(count(case when status = 'IN_TRANSIT' then 1 end), 0), 2) as last_known_cost_percent_rank_per_in_transit,
    count(*) filter (where status = 'RETURNED') as returned_rows,
    grouping_id() as grouping_level
from scored_shipments_2
group by grouping sets ((case when seq_no_2 >= 8 then 'PREMIUM' else 'STANDARD' end, case when first_known_first_known_cost >= 101 then 'PREMIUM' else 'STANDARD' end), (case when seq_no_2 >= 8 then 'PREMIUM' else 'STANDARD' end))
