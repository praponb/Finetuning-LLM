select suppliers.country, case when suppliers.rating >= 3.7 then 'high' when suppliers.rating >= 1.7 then 'mid' else 'low' end rating_bucket, sum(products.cost) total_cost
from products
inner join suppliers on products.supplier_id = suppliers.id
where products.category in ('Sports', 'Fashion')
group by suppliers.country, case when suppliers.rating >= 3.7 then 'high' when suppliers.rating >= 1.7 then 'mid' else 'low' end
having total_cost > 4300
order by total_cost
