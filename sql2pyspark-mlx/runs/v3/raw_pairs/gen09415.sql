SELECT carrier, cost, status, CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END AS is_ups, SUBSTRING(status, 1, 3) AS status_prefix FROM shipments WHERE shipped_date > '2024-08-15';
