SELECT txn_time, CASE WHEN amount >= 3300 THEN 'High' WHEN amount >= 1000 THEN 'Medium' ELSE 'Low' END amount_band FROM transactions ORDER BY amount_band
