WITH cleaned AS (
    SELECT
        *,
        DATE_ADD(opened_date, 77) opened_date_plus_77d,
        COALESCE(region, 'UNKNOWN') resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-03-16'
      AND store_name IS NULL
)
SELECT
    region,
    CASE WHEN sqft >= 17781 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(CASE WHEN region = 'East' THEN 1 END) AS east_count
FROM cleaned
GROUP BY region, CASE WHEN sqft >= 17781 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(DISTINCT id) > 1
ORDER BY region DESC;
