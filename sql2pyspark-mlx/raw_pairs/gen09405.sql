SELECT txn_type FROM transactions INTERSECT SELECT plan AS txn_type FROM subscriptions WHERE status = 'ACTIVE'
