SELECT id, shipped_date, LAST_DAY(shipped_date) AS shipped_date_month_end FROM shipments WHERE shipped_date BETWEEN '2025-02-01' AND '2025-03-20';
