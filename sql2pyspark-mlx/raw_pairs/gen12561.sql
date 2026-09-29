SELECT *, DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date FROM shipments WHERE delivered_date <= '2024-10-13' OR carrier IS NULL;
