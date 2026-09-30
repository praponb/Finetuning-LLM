select *
from products
where array_contains(tags, 'bestseller') and price <= 790
order by id desc
limit 3;
