SELECT delivered_date FROM shipments WHERE MONTH(shipped_date) IN (8, 11, 12) INTERSECT SELECT last_restock FROM inventory ORDER BY delivered_date;
