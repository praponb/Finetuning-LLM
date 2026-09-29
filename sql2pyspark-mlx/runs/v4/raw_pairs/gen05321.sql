SELECT
    i.id,
    i.quantity,
    s.region,
    s.opened_date,
    DATE_TRUNC('MONTH', i.last_restock) last_restock_period
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
WHERE YEAR(s.opened_date) >= 2023 OR s.sqft BETWEEN 6140 AND 12112
ORDER BY id DESC
