with base as (
    select
        id,
        product_id,
        quantity,
        last_restock,
        date_trunc('YEAR', last_restock) as last_restock_period
    from inventory
    where last_restock >= '2024-05-26'
),
flagged as (
    select *, case when last_restock_period - lag(last_restock_period) over (partition by product_id order by last_restock_period, id) > interval '90' day then 1 else 0 end as new_group_flag
    from base
),
sessionized as (
    select
        id,
        product_id,
        quantity,
        last_restock,
        last_restock_period,
        sum(new_group_flag) over (partition by product_id order by last_restock_period, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
scored as (
    select
        id,
        product_id,
        quantity,
        burst_id,
        lead(burst_id) over (partition by product_id order by last_restock_period, id) as next_burst_id,
        lead(quantity) over (partition by product_id order by last_restock_period, id) as next_quantity,
        lag(quantity) over (partition by product_id order by last_restock_period, id) as prev_quantity,
        row_number() over (partition by product_id order by last_restock_period, id) as row_seq
    from sessionized
),
latest as (
    select *
    from scored
    qualify row_number() over (partition by product_id order by burst_id desc, id) = 1
),
with_windows as (
    select
        id,
        product_id,
        quantity,
        burst_id,
        next_burst_id,
        next_quantity,
        prev_quantity,
        row_seq,
        avg(prev_quantity) over w_grp as last4_avg_prev_quantity,
        max(prev_quantity) over w_grp as last4_max_prev_quantity,
        sum(prev_quantity) over w_grp as last4_sum_prev_quantity
    from latest
    window w_grp as (partition by product_id order by next_quantity, id rows between 3 preceding and current row)
)
select
    product_id,
    burst_id,
    case when quantity >= 212 then 'HIGH' else 'LOW' end as quantity_tier,
    round(avg(last4_sum_prev_quantity), 2) as avg_last4_sum_prev_quantity,
    round(avg(last4_avg_prev_quantity), 2) as avg_last4_avg_prev_quantity,
    count(*) as num_rows,
    sum(row_seq) as total_row_seq
from with_windows
group by product_id, burst_id, case when quantity >= 212 then 'HIGH' else 'LOW' end
order by num_rows desc nulls last
