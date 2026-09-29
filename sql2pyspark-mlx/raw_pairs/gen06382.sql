SELECT end_date FROM subscriptions INTERSECT SELECT opened_date FROM stores WHERE opened_date >= '2023-07-01';
