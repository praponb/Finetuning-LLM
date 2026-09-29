SELECT
    ss.store_id,
    ss.product_id,
    s.store_name,
    s.opened_date,
    p.product_name,
    DAYOFWEEK(s.opened_date) AS opened_date_dayofweek
FROM store_sales ss
LEFT OUTER JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id;
