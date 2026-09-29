select p.brand, p.category, min(ss.sale_date) as first_sale_date
from store_sales ss
inner join products p on p.id = ss.product_id
inner join stores s on ss.store_id = s.id
group by p.brand, p.category
