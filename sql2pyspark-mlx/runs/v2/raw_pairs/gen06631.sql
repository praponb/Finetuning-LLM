select case when unit_price >= 360 then 'high' when unit_price >= 90 then 'mid' else 'low' end as unit_price_bucket, order_id, sum(quantity) as total_quantity from order_items group by 1, 2;
