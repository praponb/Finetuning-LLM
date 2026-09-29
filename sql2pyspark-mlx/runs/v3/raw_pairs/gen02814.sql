select a.id, a.customer_id from accounts a where a.balance > 49000 and a.customer_id not in (select id from customers where city not in ('Chiang Mai', 'Tokyo', 'New York'))
