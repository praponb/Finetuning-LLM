SELECT status, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range FROM subscriptions WHERE end_date <= '2023-09-20' AND monthly_fee >= 170 GROUP BY status;
