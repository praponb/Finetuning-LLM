select 'subscription' as kind, end_date from subscriptions where customer_id = 18 union select 'account' as kind, opened_date as end_date from accounts
