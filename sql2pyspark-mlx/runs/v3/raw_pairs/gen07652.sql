SELECT opened_date, customer_id, INITCAP(TRIM(status)) AS status_title FROM accounts WHERE status NOT IN ('OPEN', 'FROZEN') AND YEAR(opened_date) >= 2025;
