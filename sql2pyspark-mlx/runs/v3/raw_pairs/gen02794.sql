SELECT
    region,
    city,
    chiang_mai_count,
    row_count
FROM (
    SELECT
        region,
        city,
        SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END) chiang_mai_count,
        COUNT(*) row_count
    FROM stores
    GROUP BY region, city
) sub
WHERE chiang_mai_count < 31;
