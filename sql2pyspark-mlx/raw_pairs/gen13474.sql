with kept_keys as (
    select id
    from order_items
    intersect all
    select id
    from order_items
    where product_id in (24, 39)
),
kept_rows as (
    select s.*
    from order_items s
    inner join kept_keys k on s.id = k.id
)
select
    order_id,
    approx_percentile(unit_price, array(0.25, 0.5, 0.75)) as unit_price_quartiles,
    stddev_samp(unit_price) as unit_price_stddev,
    round(avg(unit_price), 2) as avg_unit_price,
    stddev_samp(quantity) as quantity_stddev,
    stddev_samp(unit_price) as unit_price_stddev_2
from kept_rows
group by rollup (order_id)
having avg_unit_price > 0;
