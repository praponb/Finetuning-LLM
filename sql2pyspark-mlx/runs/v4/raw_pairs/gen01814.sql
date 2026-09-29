SELECT *, LAST_DAY(shipped_date) shipped_date_month_end FROM shipments WHERE YEAR(delivered_date) = 2024 AND YEAR(delivered_date) = 2025;
