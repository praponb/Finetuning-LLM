select customer_id, device from sessions where hour(started_at) between 10 and 18 union select customer_id, device from sessions where started_at >= '2024-03-19';
