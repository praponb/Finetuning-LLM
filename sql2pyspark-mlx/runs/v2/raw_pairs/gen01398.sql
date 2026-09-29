select s.city, count(i.quantity) as quantity_count, count(distinct i.quantity) as unique_products
from inventory i
join products p on i.product_id = p.id
left join stores s on i.store_id = s.id
where i.last_restock between '2024-02-03' and '2025-04-01'
group by s.city
having unique_products >= 3
order by unique_products desc;
