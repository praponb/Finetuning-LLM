SELECT status, ADD_MONTHS(delivered_date, 6) AS delivered_date_plus_months FROM shipments WHERE delivered_date BETWEEN '2025-01-01' AND '2025-08-01' AND order_id IS NULL AND order_id IS NOT NULL;
