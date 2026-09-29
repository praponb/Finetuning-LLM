SELECT id, delivered_date, CONCAT_WS('-', carrier, status) AS carrier_status_key, DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_date_label FROM shipments WHERE delivered_date < '2023-04-01';
