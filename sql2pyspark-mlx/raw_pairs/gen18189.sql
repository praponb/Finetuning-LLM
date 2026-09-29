WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(supplier_name, ' - ', city) AS supplier_name_city,
        LENGTH(supplier_name) AS supplier_name_length
    FROM suppliers
    WHERE city IS NOT NULL
)
-- Grouped metrics
SELECT
    city,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) AS jp_rating,
    COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows
FROM filtered
GROUP BY city
ORDER BY jp_rows DESC
