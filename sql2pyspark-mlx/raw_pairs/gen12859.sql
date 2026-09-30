SELECT opened_date FROM accounts UNION ALL SELECT shipped_date FROM shipments WHERE status = 'DELIVERED' ORDER BY opened_date;
