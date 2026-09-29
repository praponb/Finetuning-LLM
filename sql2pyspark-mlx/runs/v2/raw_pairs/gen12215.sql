select s.id, s.end_date, s.monthly_fee from subscriptions s where s.customer_id in (select id from customers where segment = 'Consumer')
