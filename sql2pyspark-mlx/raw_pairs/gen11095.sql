select
    order_id,
    count(*) as cnt,
    percentile_approx(unit_price, array(0.25, 0.5, 0.75)) as unit_price_quartiles,
    avg(unit_price) as avg_unit_price,
    min(unit_price) as min_unit_price,
    corr(quantity, unit_price) as corr_quantity_unit_price
from order_items
group by 1
having corr(quantity, unit_price) >= 0
order by corr_quantity_unit_price desc nulls last
