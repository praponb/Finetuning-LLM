SELECT plan, AVG(monthly_fee) avg_monthly_fee FROM subscriptions WHERE end_date BETWEEN '2024-07-01' AND '2025-04-01' AND monthly_fee >= 180 GROUP BY plan ORDER BY avg_monthly_fee;
