select status, 'account' as source from accounts union select status, 'subscription' as source from subscriptions where end_date between '2025-02-01' and '2025-05-01'
