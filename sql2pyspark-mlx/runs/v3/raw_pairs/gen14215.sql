SELECT *, LENGTH(status) AS status_length, DATE_TRUNC('YEAR', delivered_date) AS delivered_date_period
FROM shipments
