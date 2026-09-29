SELECT status, INITCAP(TRIM(status)) AS status_title, CONCAT_WS('-', carrier, status) AS carrier_status_key FROM shipments;
