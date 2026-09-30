SELECT delivered_date FROM shipments UNION ALL SELECT opened_date FROM stores ORDER BY delivered_date DESC;
