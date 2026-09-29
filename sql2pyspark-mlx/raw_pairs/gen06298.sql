select 'subscription' as origin, monthly_fee from subscriptions union all select 'order' as origin, amount from orders order by origin;
