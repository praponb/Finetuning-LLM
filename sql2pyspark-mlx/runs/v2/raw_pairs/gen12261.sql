SELECT id, customer_id, CASE WHEN monthly_fee >= 205 THEN 'High' WHEN monthly_fee >= 41 THEN 'Medium' ELSE 'Low' END AS monthly_fee_band FROM subscriptions;
