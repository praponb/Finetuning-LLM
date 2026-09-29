SELECT cost FROM shipments WHERE MONTH(delivered_date) IN (2, 5, 10) UNION SELECT monthly_fee FROM subscriptions ORDER BY cost DESC
