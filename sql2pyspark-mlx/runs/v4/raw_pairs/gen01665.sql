select status, 'account' as source from accounts where status is null union select status, 'subscription' as source from subscriptions order by status desc
