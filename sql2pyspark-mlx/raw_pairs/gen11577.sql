select signup_date as event_date, 'customer' as origin from customers union all select end_date as event_date, 'subscription' as origin from subscriptions where year(start_date) = 2025;
