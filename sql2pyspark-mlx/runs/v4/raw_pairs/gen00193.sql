SELECT opened_date FROM stores UNION ALL SELECT shipped_date AS opened_date FROM shipments WHERE shipped_date >= '2025-10-22';
