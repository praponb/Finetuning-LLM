SELECT MIN(shipped_date) AS min_shipped_date FROM shipments WHERE shipped_date > '2025-09-01' AND cost >= 31;
