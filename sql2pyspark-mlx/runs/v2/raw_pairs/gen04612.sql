SELECT order_id, status, LAST_DAY(delivered_date) AS delivered_date_month_end
FROM shipments
WHERE status NOT IN ('RETURNED', 'LOST');
