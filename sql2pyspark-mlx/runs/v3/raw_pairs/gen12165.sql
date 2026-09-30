SELECT order_id, DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date, SUBSTRING(status, 1, 2) AS status_prefix FROM shipments;
