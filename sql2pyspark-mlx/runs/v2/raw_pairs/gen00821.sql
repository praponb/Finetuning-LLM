SELECT status, MAX(delivered_date) AS latest_delivered_date FROM shipments WHERE status IN ('LOST', 'RETURNED', 'IN_TRANSIT') AND carrier = 'FedEx' GROUP BY status;
