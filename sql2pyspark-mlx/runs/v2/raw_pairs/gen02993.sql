select case when quantity >= 265 then 'high' when quantity >= 118 then 'mid' else 'low' end as quantity_bucket, sum(quantity) as total_quantity from inventory group by 1;
