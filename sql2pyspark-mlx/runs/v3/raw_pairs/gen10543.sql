select quantity_bucket, product_id, quantity_count
from (
    select case when quantity >= 181 then 'high' when quantity >= 53 then 'mid' else 'low' end as quantity_bucket, product_id, count(quantity) as quantity_count
    from inventory
    where product_id in (5, 6, 8, 9)
    group by case when quantity >= 181 then 'high' when quantity >= 53 then 'mid' else 'low' end, product_id
) as sub
where quantity_count < 9
order by product_id desc, quantity_count;
