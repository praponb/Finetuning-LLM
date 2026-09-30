SELECT id, opened_date, LAST_DAY(opened_date) opened_date_month_end
FROM stores
WHERE sqft = 5440 OR city NOT IN ('Tokyo', 'Bangkok')
ORDER BY opened_date
