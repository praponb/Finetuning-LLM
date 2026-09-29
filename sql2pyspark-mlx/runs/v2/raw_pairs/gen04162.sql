select status from accounts except select status from subscriptions where customer_id is not null order by status
