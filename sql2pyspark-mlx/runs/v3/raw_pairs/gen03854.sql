select subscriptions.id, subscriptions.monthly_fee
from subscriptions
where subscriptions.customer_id in (select id from customers where segment <> 'SMB')
order by monthly_fee desc, subscriptions.id asc
limit 5;
