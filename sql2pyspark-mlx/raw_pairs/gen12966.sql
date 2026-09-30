select id, end_date, monthly_fee from subscriptions where end_date = (select max(end_date) from subscriptions where plan in ('standard', 'premium', 'basic'))
