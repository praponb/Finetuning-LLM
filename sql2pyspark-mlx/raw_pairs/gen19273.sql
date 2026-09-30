SELECT shipped_date AS event_date, 'shipment' AS origin FROM shipments UNION ALL SELECT hire_date AS event_date, 'employee' AS origin FROM employees;
