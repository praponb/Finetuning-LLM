with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        quantity,
        unit_price,
        quantity * unit_price line_total
    from order_items
    where product_id is not null
      and product_id = 31
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        product_id,
        count(line_total) over (partition by product_id order by quantity, id rows between 2 preceding and current row) last3_count_line_total,
        min(line_total) over (partition by product_id order by quantity, id rows between 2 preceding and current row) last3_min_line_total,
        sum(line_total) over (partition by product_id order by quantity, id rows between 2 preceding and current row) last3_sum_line_total,
        corr(unit_price, quantity) over (partition by product_id) corr_unit_price_quantity
    from filtered
    where line_total < 65
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        last3_count_line_total,
        last3_min_line_total,
        last3_sum_line_total,
        floor(last3_min_line_total) last3_min_line_total_whole
    from with_windows
    where last3_min_line_total between 24 and 34
),
base_order_items as (
    -- Keep recent, valid rows
    select *, round(last3_min_line_total * 1.15, 2) adjusted_last3_min_line_total
    from base
    where product_id = 23
),
report as (
    select
        id,
        last3_count_line_total,
        last3_min_line_total,
        last3_min_line_total_whole,
        adjusted_last3_min_line_total
    from base_order_items
)
select
    id,
    last3_count_line_total,
    last3_min_line_total,
    last3_min_line_total_whole
from report
order by last3_count_line_total nulls first
