select status from subscriptions where monthly_fee between 120 and 220 except select status from subscriptions where customer_id is null order by status
