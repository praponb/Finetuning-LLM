select p.brand, count(*) as inventory_count, avg(i.quantity) as avg_quantity
from inventory i
inner join stores s on i.store_id = s.id
inner join products p on i.product_id = p.id
where p.supplier_id = 23
group by p.brand
having count(*) >= 1
order by p.brand
limit 50;
