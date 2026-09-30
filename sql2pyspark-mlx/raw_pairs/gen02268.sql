WITH valid AS (
    SELECT
        *,
        INITCAP(TRIM(region)) AS region_title,
        LOWER(store_name) AS store_name_lower,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE opened_date > DATE '2023-06-27'
      AND city = 'London'
)
SELECT
    id,
    city,
    opened_date,
    region_title,
    store_name_lower,
    COUNT(sqft) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_sqft,
    MIN(sqft) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_sqft,
    AVG(sqft) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_sqft,
    LEAD(sqft, 1, 0) OVER (ORDER BY opened_date, id) AS next_sqft
FROM valid;
