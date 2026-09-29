with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        last_restock,
        ceil(quantity) quantity_whole
    from inventory
    where quantity is not null
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        last_restock,
        quantity_whole,
        date_format(last_restock, 'MMM yyyy') last_restock_label,
        last_day(last_restock) last_restock_month_end
    from valid
    where last_restock >= to_date('2023-03-28')
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        quantity_whole,
        last_restock_label,
        round(quantity_whole, 1) quantity_whole_rounded,
        ceil(quantity_whole) quantity_whole_whole
    from base
    where last_restock >= date '2023-02-09'
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lag(quantity_whole_rounded) over w_grp prev_quantity_whole_rounded,
        lead(quantity_whole) over w_grp next_quantity_whole,
        row_number() over w_grp row_seq,
        row_number() over (order by quantity_whole_whole, id) seq_no
    from cleaned
    window w_grp as (order by quantity_whole_rounded, id)
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        quantity_whole,
        last_restock_label,
        quantity_whole_whole,
        next_quantity_whole,
        row_seq,
        seq_no,
        quantity_whole - quantity_whole_rounded quantity_whole_minus_quantity_whole_rounded,
        quantity_whole_rounded - row_seq quantity_whole_rounded_minus_row_seq
    from metrics
    where quantity_whole != 21
),
base_inventory as (
    -- Keep recent, valid rows
    select
        id,
        quantity_whole,
        last_restock_label,
        quantity_whole_whole,
        next_quantity_whole,
        row_seq,
        seq_no,
        quantity_whole_minus_quantity_whole_rounded,
        least(quantity_whole_whole, quantity_whole_rounded_minus_row_seq) least_quantity_whole_whole_quantity_whole_rounded_minus_row_seq
    from filtered
    where quantity_whole_minus_quantity_whole_rounded between 21 and 70
)
-- Running and moving aggregates
select
    id,
    quantity_whole,
    last_restock_label,
    seq_no,
    quantity_whole_minus_quantity_whole_rounded,
    avg(quantity_whole_minus_quantity_whole_rounded) over (order by seq_no, id rows between 2 preceding and 1 preceding) quantity_whole_minus_quantity_whole_rounded_avg_2_prior,
    cume_dist() over (order by seq_no nulls first) seq_no_cume_dist,
    percent_rank() over (order by seq_no desc nulls first) seq_no_percent_rank
from base_inventory
