select status, 'account' as record_type from accounts where month(opened_date) in (3, 11, 12) union select status, 'subscription' as record_type from subscriptions where customer_id in (15, 22)
