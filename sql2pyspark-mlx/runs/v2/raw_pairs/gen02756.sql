SELECT id, delivered_date, cost, DATE_TRUNC('WEEK', delivered_date) AS delivered_date_period FROM shipments
