SELECT start_date, plan, CONCAT_WS('-', plan, status) plan_status_key FROM subscriptions WHERE monthly_fee >= 190 OR monthly_fee BETWEEN 130 AND 220 ORDER BY plan_status_key, start_date
