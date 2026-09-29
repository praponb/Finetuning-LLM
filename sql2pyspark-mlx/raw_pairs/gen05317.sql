select p.brand, max(i.quantity) - min(i.quantity) as quantity_range, max(i.last_restock) as latest_last_restock
from inventory i
inner join products p on i.product_id = p.id
where i.quantity >= 163
group by p.brand
order by quantity_range, brand
limit 20;
