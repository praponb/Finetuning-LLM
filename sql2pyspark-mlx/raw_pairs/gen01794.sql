select *, floor(monthly_fee) as monthly_fee_whole, date_trunc('YEAR', end_date) as end_date_period from subscriptions where customer_id in (4, 5, 10, 14);
