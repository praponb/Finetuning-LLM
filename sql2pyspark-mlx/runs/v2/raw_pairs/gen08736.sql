SELECT id, customer_id, plan, end_date FROM subscriptions WHERE monthly_fee > 240 AND start_date < '2024-02-11' ORDER BY 4, customer_id DESC, id LIMIT 20;
