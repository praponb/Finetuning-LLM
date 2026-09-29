with metrics as (
    select
        *,
        ntile(10) over w_part as unit_price_bucket,
        round(quantity / sum(quantity) over (partition by order_id), 4) as quantity_share,
        sum(quantity) over w_grp as running_quantity
    from order_items
    window w_part as (partition by product_id order by unit_price desc, id), w_grp as (partition by product_id, order_id order by unit_price, id rows between unbounded preceding and current row)
)
select
    order_id,
    approx_percentile(running_quantity, 0.9) as running_quantity_p90,
    percentile_approx(quantity_share, 0.9) as quantity_share_p90,
    count(distinct order_id) as distinct_order_ids,
    round(avg(quantity_share), 2) as avg_quantity_share,
    percentile_approx(unit_price, array(0.25, 0.5, 0.75)) as unit_price_quartiles,
    bit_or(quantity) as quantity_bit_or
from metrics
group by order_id
order by avg_quantity_share desc
