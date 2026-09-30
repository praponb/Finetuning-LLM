with latest as (
    select id, quantity
    from (
        select *, row_number() over (partition by store_id order by quantity desc nulls first, id) as row_num
        from inventory
    ) t
    where row_num = 1
),
base as (
    select *, round(quantity * 1.15, 2) as quantity_adjusted
    from latest
    where quantity is not null
),
enriched as (
    select
        *,
        sum(quantity) over (order by quantity, id rows between unbounded preceding and current row) as running_quantity,
        avg(quantity_adjusted) over (order by quantity, id rows between 3 preceding and current row) as quantity_adjusted_avg_3,
        lead(quantity_adjusted, 1) over (order by quantity_adjusted, id) as next_quantity_adjusted
    from base
),
metrics as (
    select
        *,
        min(quantity_adjusted) over (order by quantity_adjusted, id rows between 3 preceding and current row) as last4_min_quantity_adjusted,
        sum(quantity_adjusted) over (order by quantity_adjusted, id rows between 3 preceding and current row) as last4_sum_quantity_adjusted,
        count(quantity_adjusted) over (order by quantity_adjusted, id rows between 3 preceding and current row) as last4_count_quantity_adjusted,
        max(quantity_adjusted) over (order by quantity_adjusted, id rows between 3 preceding and current row) as last4_max_quantity_adjusted,
        sum(running_quantity) over (order by quantity_adjusted, id rows between unbounded preceding and current row) as running_running_quantity
    from enriched
),
scored as (
    select
        id,
        quantity,
        quantity_adjusted_avg_3,
        running_running_quantity,
        lead(next_quantity_adjusted) over w_ordered as next_next_quantity_adjusted,
        lag(running_running_quantity) over w_ordered as prev_running_running_quantity,
        lead(running_running_quantity) over w_ordered as next_running_running_quantity,
        row_number() over w_ordered as seq_no,
        row_number() over w_grp as seq_no_2
    from metrics
    where next_quantity_adjusted <= 38
    window w_ordered as (order by last4_min_quantity_adjusted, id), w_grp as (order by quantity_adjusted desc, id)
),
scored_inventory as (
    select
        *,
        last_value(seq_no) ignore nulls over (order by running_running_quantity, id rows between unbounded preceding and current row) as last_known_seq_no,
        sum(next_running_running_quantity) over (order by quantity, id rows between unbounded preceding and current row) as running_next_running_running_quantity
    from scored
),
base_inventory as (
    select
        id,
        quantity,
        quantity_adjusted_avg_3,
        next_next_quantity_adjusted,
        prev_running_running_quantity,
        next_running_running_quantity,
        seq_no,
        seq_no_2,
        last_known_seq_no,
        quantity_adjusted_avg_3 * last_known_seq_no as quantity_adjusted_avg_3_times_last_known_seq_no
    from scored_inventory
    where quantity >= 145
)
select
    case when next_next_quantity_adjusted >= 76 then 'PREMIUM' else 'STANDARD' end as next_next_quantity_adjusted_tier,
    approx_percentile(quantity_adjusted_avg_3, 0.95) as quantity_adjusted_avg_3_p95,
    count(distinct id) as distinct_ids,
    bit_or(quantity) as quantity_bit_or,
    bit_or(quantity) as quantity_bit_or_2,
    round(avg(seq_no_2), 2) as avg_seq_no_2
from base_inventory
group by 1
