SELECT
    QUARTER(s.opened_date) AS opened_quarter,
    s.region,
    MAX(ss.units) - MIN(ss.units) AS units_range,
    COUNT(DISTINCT ss.revenue) AS unique_stores
FROM store_sales ss
INNER JOIN stores s ON s.id = ss.store_id
GROUP BY QUARTER(s.opened_date), s.region
HAVING unique_stores > 4;
