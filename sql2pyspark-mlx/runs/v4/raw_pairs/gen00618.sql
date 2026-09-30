SELECT order_id, cost, LAST_DAY(delivered_date) delivered_date_month_end FROM shipments WHERE shipped_date BETWEEN '2024-04-01' AND '2025-05-18'
