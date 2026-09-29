select date_trunc('MONTH', stores.opened_date) as opened_month, sort_array(collect_set(orders.channel)) as channel_list
from orders
left outer join customers on customers.id = orders.customer_id
join stores on stores.id = orders.store_id
where lower(trim(stores.store_name)) = 'old town'
group by date_trunc('MONTH', stores.opened_date)
order by opened_month desc
