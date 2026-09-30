select id, monthly_fee, start_date, customer_id from subscriptions where monthly_fee > (select max(monthly_fee) from subscriptions) order by id limit 50;
