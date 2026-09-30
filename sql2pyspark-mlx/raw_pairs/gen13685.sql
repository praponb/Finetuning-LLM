select
    order_id,
    case when quantity >= 7 then 'LARGE' else 'SMALL' end as quantity_tier,
    product_id,
    stddev_samp(quantity) as quantity_stddev,
    stddev_samp(unit_price) as unit_price_stddev,
    percentile_approx(unit_price, 0.9) as unit_price_p90,
    round(avg(unit_price), 2) as avg_unit_price,
    stddev_samp(unit_price) as unit_price_stddev_2,
    sum(unit_price) as total_unit_price,
    grouping(order_id) as grouping_level
from order_items
group by cube (order_id, case when quantity >= 7 then 'LARGE' else 'SMALL' end, product_id)
having percentile_approx(unit_price, 0.9) >= 0
order by total_unit_price
