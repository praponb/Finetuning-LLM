select product_id, case when quantity >= 7 then 'high' when quantity >= 2 then 'mid' else 'low' end as quantity_bucket, count(*) as num_order_items
from order_items
where quantity is not null and product_id = 34
group by product_id, quantity_bucket
order by product_id desc;
