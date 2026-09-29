SELECT carrier, MIN(shipped_date) AS first_shipped_date FROM shipments WHERE status = 'DELIVERED' GROUP BY carrier
