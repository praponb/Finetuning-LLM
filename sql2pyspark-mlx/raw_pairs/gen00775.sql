with deduped as (
    -- QUALIFY-style window filter
    select *
    from inventory
    qualify row_number() over (partition by product_id, store_id order by last_restock desc, id) = 1
),
scored as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by product_id order by last_restock desc, id) as seq_no,
        lead(quantity, 1) over (partition by product_id order by last_restock, id) as next_quantity,
        avg(quantity) over (partition by store_id) as store_id_avg_quantity,
        dense_rank() over (order by quantity desc nulls last) as quantity_dense_rank
    from deduped
),
deduped_inventory as (
    -- Deduplicate with ROW_NUMBER
    select *
    from scored
    qualify row_number() over (partition by store_id order by quantity_dense_rank desc, id) = 1
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(quantity_dense_rank) over (partition by year(last_restock), product_id order by quantity_dense_rank, id rows between unbounded preceding and current row) as cumulative_quantity_dense_rank,
        lead(quantity, 1) over (order by store_id_avg_quantity, id) as next_quantity_2,
        first_value(seq_no) ignore nulls over (partition by month(last_restock), store_id order by quantity_dense_rank, id rows between unbounded preceding and current row) as first_known_seq_no
    from deduped_inventory
),
metrics as (
    -- Ranking and gap features
    select
        id,
        product_id,
        quantity,
        last_restock,
        seq_no,
        next_quantity,
        store_id_avg_quantity,
        quantity_dense_rank,
        next_quantity_2,
        first_known_seq_no,
        ntile(3) over (order by quantity_dense_rank, id) as quantity_dense_rank_bucket,
        rank() over (partition by product_id order by quantity_dense_rank desc) as quantity_dense_rank_rank,
        last_value(seq_no) ignore nulls over (order by quantity, id rows between unbounded preceding and current row) as last_known_seq_no,
        lead(first_known_seq_no, 1) over (partition by month(last_restock), product_id order by cumulative_quantity_dense_rank, id) as next_first_known_seq_no
    from with_windows
),
daily as (
    -- Multi-level aggregation
    select
        date_trunc('year', last_restock) as year_start,
        case when quantity_dense_rank >= 8 then 'PREMIUM' else 'STANDARD' end as quantity_dense_rank_tier,
        bit_and(quantity) as quantity_bit_and,
        round(avg(quantity_dense_rank), 2) as avg_quantity_dense_rank,
        round(avg(first_known_seq_no), 2) as avg_first_known_seq_no
    from metrics
    group by rollup (date_trunc('year', last_restock), case when quantity_dense_rank >= 8 then 'PREMIUM' else 'STANDARD' end)
)
select
    year_start,
    quantity_bit_and,
    avg_quantity_dense_rank
from daily;
