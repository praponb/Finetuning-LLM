select p.id, p.cost, p.price
from products p
where p.price > 650 and not exists (select 1 from store_sales ss where ss.product_id = p.id and ss.units >= 17)
order by price desc;
