select status, 'payment' as origin from payments where order_id in (5, 14, 23) union all select status, 'subscription' as origin from subscriptions where plan <> 'premium' order by origin;
