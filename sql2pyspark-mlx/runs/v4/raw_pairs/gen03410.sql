select case when suppliers.rating >= 4.3 then 'high' when suppliers.rating >= 2.2 then 'mid' else 'low' end as rating_bucket, sum(products.cost) as total_cost
from products
inner join suppliers on products.supplier_id = suppliers.id
where products.brand is null
group by case when suppliers.rating >= 4.3 then 'high' when suppliers.rating >= 2.2 then 'mid' else 'low' end
