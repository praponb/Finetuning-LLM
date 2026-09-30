select p.brand, ss.sale_date, trim(p.category) as clean_category
from products p
left join store_sales ss on p.id = ss.product_id
where p.supplier_id = 25
order by clean_category;
