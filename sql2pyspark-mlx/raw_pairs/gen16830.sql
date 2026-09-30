SELECT *, LENGTH(status) AS status_length, WEEKOFYEAR(delivered_date) AS delivered_date_weekofyear FROM shipments WHERE status = 'LOST' AND status = 'DELIVERED' AND cost <= 77;
