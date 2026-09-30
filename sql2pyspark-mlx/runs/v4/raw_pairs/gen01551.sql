SELECT QUARTER(shipped_date) shipped_quarter, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) carrier_values, SUM(CASE WHEN status = 'DELIVERED' THEN cost ELSE 0 END) delivered_cost
FROM shipments
WHERE carrier = 'UPS' AND (MONTH(shipped_date) IN (3, 8, 12) OR delivered_date > '2025-03-01')
GROUP BY QUARTER(shipped_date)
HAVING SUM(CASE WHEN status = 'IN_TRANSIT' THEN 1 ELSE 0 END) > 26;
