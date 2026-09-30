select product_id, avg_units
from (
    select product_id, round(avg(units), 2) as avg_units
    from store_sales
    group by product_id
) as x
where avg_units > 53
order by product_id desc
