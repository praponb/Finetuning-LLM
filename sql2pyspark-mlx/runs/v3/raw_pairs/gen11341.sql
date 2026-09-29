select product_id, case when quantity >= 7 then 'high' when quantity >= 3 then 'mid' else 'low' end as quantity_bucket, sum(unit_price * quantity) as revenue
from order_items
group by rollup(product_id, case when quantity >= 7 then 'high' when quantity >= 3 then 'mid' else 'low' end)
order by revenue desc, quantity_bucket
