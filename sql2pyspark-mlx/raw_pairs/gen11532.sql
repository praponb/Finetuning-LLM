SELECT status AS label FROM subscriptions INTERSECT SELECT txn_type AS label FROM transactions ORDER BY label
