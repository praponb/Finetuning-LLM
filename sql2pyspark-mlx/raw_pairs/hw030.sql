SELECT device, COUNT(DISTINCT customer_id) AS unique_visitors FROM sessions GROUP BY device;
