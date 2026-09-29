select s.city, max(i.last_restock) as last_last_restock
from inventory i
left join products p on i.product_id = p.id
inner join stores s on i.store_id = s.id
group by s.city
