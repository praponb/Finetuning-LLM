select products.id, products.category, count(store_sales.id) as store_sale_count
from products
join store_sales on products.id = store_sales.product_id
where store_sales.revenue >= 1900
group by products.id, products.category
order by store_sale_count, id, category
limit 50;
