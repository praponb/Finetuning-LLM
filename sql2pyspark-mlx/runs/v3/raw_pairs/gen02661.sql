SELECT plan, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range FROM subscriptions WHERE MONTH(end_date) IN (4, 6, 12) AND customer_id IS NULL GROUP BY plan ORDER BY 2 ASC
