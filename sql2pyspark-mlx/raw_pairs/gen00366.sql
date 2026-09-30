SELECT
    ss.sale_date,
    ss.store_id,
    s.city,
    s.region,
    DATE_TRUNC('month', s.opened_date) AS opened_date_period
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
WHERE ss.sale_date > '2023-07-01';
