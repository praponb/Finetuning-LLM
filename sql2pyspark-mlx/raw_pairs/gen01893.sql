SELECT
    s.sqft,
    ss.sale_date,
    ss.store_id,
    ss.revenue,
    ROUND(ss.units, 1) AS units_rounded
FROM stores s
LEFT JOIN store_sales ss ON ss.store_id = s.id;
