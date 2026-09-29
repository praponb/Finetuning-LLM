select ss.product_id, count(i.quantity) as quantity_count
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.sale_date <= '2025-06-28'
group by ss.product_id
having quantity_count > 5
order by product_id
limit 5;
