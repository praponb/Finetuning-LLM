SELECT status FROM accounts WHERE opened_date >= '2023-03-29' UNION ALL SELECT status FROM shipments WHERE YEAR(shipped_date) = 2025 ORDER BY status;
