with remaining_keys as (
    -- Set intersection on keys
    select store_id
    from orders
    intersect
    select store_id
    from orders
    where shipping_fee > 28
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from orders s
    inner join remaining_keys k on s.store_id = k.store_id
)
-- Multi-level aggregation
select
    date_trunc('year', order_date) as year_start,
    approx_percentile(discount, 0.9) as discount_p90,
    percentile_approx(amount, 0.5) as amount_p50
from kept_rows
group by date_trunc('year', order_date)
order by amount_p50 desc;
