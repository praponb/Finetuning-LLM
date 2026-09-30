select case when suppliers.rating >= 4.6 then 'high' when suppliers.rating >= 2.1 then 'mid' else 'low' end as rating_bucket, count(distinct order_items.unit_price) as unique_orders, count(*) as order_item_count
from order_items
inner join products on order_items.product_id = products.id
left join suppliers on products.supplier_id = suppliers.id
where suppliers.supplier_name like 'Lio%'
group by case when suppliers.rating >= 4.6 then 'high' when suppliers.rating >= 2.1 then 'mid' else 'low' end
