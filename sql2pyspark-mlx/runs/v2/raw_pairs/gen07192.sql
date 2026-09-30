select
    ss.product_id,
    ss.store_id,
    count(i.quantity) as quantity_count,
    count(*) as cnt
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.store_id in (2, 4)
group by ss.product_id, ss.store_id
order by quantity_count, cnt, product_id, store_id
limit 5;
