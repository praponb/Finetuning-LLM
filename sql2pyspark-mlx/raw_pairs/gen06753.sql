SELECT plan FROM subscriptions INTERSECT SELECT plan FROM subscriptions WHERE MONTH(end_date) IN (1, 3, 8)
