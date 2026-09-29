select
    case when stores.sqft >= 14371 then 'high' when stores.sqft >= 3464 then 'mid' else 'low' end as sqft_bucket,
    stores.region,
    max(orders.shipping_fee) - min(orders.shipping_fee) as shipping_fee_range,
    min(orders.order_date) as first_order_date
from orders
join customers on orders.customer_id = customers.id
right join stores on orders.store_id = stores.id
where orders.discount between 0.11 and 0.39
group by case when stores.sqft >= 14371 then 'high' when stores.sqft >= 3464 then 'mid' else 'low' end, stores.region
order by shipping_fee_range desc;
