select 'customer' as record_type, signup_date from customers where signup_date is not null union all select 'subscription' as record_type, start_date from subscriptions where start_date is not null;
