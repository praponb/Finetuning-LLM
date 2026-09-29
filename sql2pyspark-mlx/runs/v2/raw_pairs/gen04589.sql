SELECT plan, end_date FROM subscriptions WHERE monthly_fee >= 110 UNION ALL SELECT plan, end_date FROM subscriptions WHERE end_date > '2024-05-01'
