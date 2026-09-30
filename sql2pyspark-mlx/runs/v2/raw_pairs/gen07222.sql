select s.id, s.monthly_fee from subscriptions s where s.customer_id in (select id from customers where country in ('US', 'TH'))
