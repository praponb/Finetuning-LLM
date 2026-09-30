SELECT id, monthly_fee, DENSE_RANK() OVER (ORDER BY monthly_fee) AS dense_rnk FROM subscriptions ORDER BY dense_rnk, monthly_fee DESC;
