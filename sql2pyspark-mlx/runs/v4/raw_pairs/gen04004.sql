select status from accounts where status in ('FROZEN', 'OPEN') intersect select status from subscriptions;
