select
    case when stores.sqft >= 14057 then 'high' when stores.sqft >= 6598 then 'mid' else 'low' end as sqft_bucket,
    stores.city,
    sort_array(collect_set(orders.channel)) as channel_list,
    count(distinct orders.shipping_fee) as unique_statuss
from orders
join stores on stores.id = orders.store_id
join customers on customers.id = orders.customer_id
where stores.region not in ('Central', 'North')
group by case when stores.sqft >= 14057 then 'high' when stores.sqft >= 6598 then 'mid' else 'low' end, stores.city
having count(distinct orders.shipping_fee) >= 2
order by sqft_bucket, unique_statuss desc, city
limit 1;
