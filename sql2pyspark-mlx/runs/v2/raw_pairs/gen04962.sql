SELECT id, end_date, ROW_NUMBER() OVER (ORDER BY end_date DESC, id) end_date_row
FROM subscriptions
WHERE customer_id IN (8, 25, 37)
