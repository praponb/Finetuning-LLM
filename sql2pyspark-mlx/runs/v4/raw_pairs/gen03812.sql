select year(s.opened_date) as opened_year, max(i.last_restock) as last_last_restock, max(i.last_restock) as latest_last_restock
from inventory i
inner join products p on i.product_id = p.id
left outer join stores s on i.store_id = s.id
group by year(s.opened_date);
