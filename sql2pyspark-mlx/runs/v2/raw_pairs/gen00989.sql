SELECT * FROM (SELECT plan, MIN(start_date) AS min_start_date, COUNT(DISTINCT monthly_fee) AS unique_customers FROM subscriptions GROUP BY plan) AS t WHERE unique_customers >= 6;
