with filtered as (
    -- Keep recent, valid rows
    select *, floor(unit_price) as unit_price_whole
    from order_items
    where quantity between 3 and 6
)
-- Aggregate with conditional metrics
select
    order_id,
    count(distinct order_id) as distinct_order_ids,
    stddev_samp(unit_price) as unit_price_stddev,
    bit_or(quantity) as quantity_bit_or,
    bit_and(quantity) as quantity_bit_and
from filtered
group by 1;
