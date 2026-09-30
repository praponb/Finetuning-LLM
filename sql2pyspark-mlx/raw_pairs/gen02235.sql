select id, plan, start_date, customer_id, first_value(customer_id) over (partition by plan order by start_date, id) as first_customer_id from subscriptions;
