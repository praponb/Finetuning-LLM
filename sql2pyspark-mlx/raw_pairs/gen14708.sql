SELECT *, TO_DATE(started_at) started_at_date, LENGTH(country) country_length
FROM sessions
WHERE browser IN ('Chrome', 'Firefox', 'Edge') AND (customer_id IS NOT NULL OR customer_id IS NOT NULL)
