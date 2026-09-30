WITH FilteredSuppliers AS (
    SELECT *
    FROM suppliers
    WHERE rating <= 2.8 OR country = 'UK'
),
suppliers_stats AS (
    SELECT
        city,
        CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END rating_bucket,
        SUM(CASE WHEN country = 'UK' THEN 1 ELSE 0 END) uk_count,
        COUNT(rating) rating_count
    FROM FilteredSuppliers
    GROUP BY city, CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END
)
SELECT
    city,
    rating_bucket,
    uk_count,
    rating_count
FROM suppliers_stats;
