select p.id, p.brand, count(ss.id) as store_sale_count
from products p
left join store_sales ss on ss.product_id = p.id
group by p.id, p.brand
order by p.brand, p.id desc
limit 10;
