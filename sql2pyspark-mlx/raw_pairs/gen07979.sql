with metrics as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over w_part as row_seq,
        lag(quantity, 2, 0) over w_part as prev_quantity,
        lag(quantity, 1) over w_ordered as prev_quantity_2
    from inventory
    where last_restock > '2024-06-01'
    window w_part as (partition by store_id order by last_restock, id), w_ordered as (partition by store_id order by quantity, id)
),
joined as (
    -- Attach products attributes
    select
        s.id,
        s.product_id,
        s.quantity,
        s.row_seq,
        s.prev_quantity,
        s.prev_quantity_2,
        p.product_name
    from metrics s
    join products p
        on s.product_id = p.id
),
kept_keys as (
    -- Set intersection on keys
    select id
    from joined
    intersect all
    select id
    from inventory
    where store_id in (1, 2, 6)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    join kept_keys k on s.id = k.id
),
metrics_inventory as (
    -- Running and moving aggregates
    select
        id,
        quantity,
        product_name,
        rank() over (order by row_seq nulls first) as row_seq_rank,
        sum(prev_quantity_2) over (partition by product_id) as product_id_sum_prev_quantity_2
    from reconciled
)
-- Grouped metrics
select
    case when product_id_sum_prev_quantity_2 >= 1079 then 'LARGE' else 'SMALL' end as product_id_sum_prev_quantity_2_tier,
    bit_or(quantity) as quantity_bit_or,
    approx_percentile(row_seq_rank, array(0.25, 0.5, 0.75)) as row_seq_rank_quartiles,
    bit_and(quantity) as quantity_bit_and,
    count(distinct id) as distinct_ids,
    approx_percentile(row_seq_rank, array(0.25, 0.5, 0.75)) as row_seq_rank_quartiles_2,
    bit_or(quantity) as quantity_bit_or_2
from metrics_inventory
group by case when product_id_sum_prev_quantity_2 >= 1079 then 'LARGE' else 'SMALL' end
order by distinct_ids;
