select
    case when unit_price >= 420 then 'high' when unit_price >= 90 then 'mid' else 'low' end unit_price_bucket,
    max(unit_price) max_unit_price,
    count(quantity) quantity_count,
    round(stddev(unit_price), 2) stddev_unit_price
from order_items
where unit_price >= 140
group by 1
order by unit_price_bucket, stddev_unit_price desc
limit 20
