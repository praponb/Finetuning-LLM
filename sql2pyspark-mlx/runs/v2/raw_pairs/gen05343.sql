SELECT QUARTER(delivered_date) AS delivered_quarter, SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list FROM shipments GROUP BY delivered_quarter HAVING COUNT(*) >= 3;
