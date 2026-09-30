SELECT order_id, SUM(CASE WHEN status = 'IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit_count, MAX(shipped_date) AS latest_shipped_date FROM shipments GROUP BY order_id ORDER BY in_transit_count DESC;
