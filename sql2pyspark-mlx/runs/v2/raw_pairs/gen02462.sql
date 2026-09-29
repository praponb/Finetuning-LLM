SELECT
    ss.id,
    ss.revenue,
    ss.sale_date,
    s.opened_date
FROM store_sales ss
LEFT OUTER JOIN stores s ON ss.store_id = s.id
WHERE s.sqft IS NOT NULL AND s.city IN ('New York', 'Chiang Mai');
