WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        CASE WHEN rating >= 4.6 THEN 'High' WHEN rating >= 1.5 THEN 'Medium' ELSE 'Low' END rating_tier,
        COALESCE(city, 'OTHER') city_filled
    FROM suppliers
    WHERE rating <= 4.2
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        LENGTH(country) country_length
    FROM cleaned
    WHERE city NOT IN ('Bangkok', 'London', 'New York')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        city,
        country_length,
        AVG(country_length) OVER (PARTITION BY city ORDER BY country_length ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) country_length_avg_2_prior,
        SUM(country_length) OVER (PARTITION BY city ORDER BY country_length ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_country_length,
        SUM(country_length) OVER (PARTITION BY country ORDER BY country_length ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_country_length,
        CUME_DIST() OVER (ORDER BY country_length DESC NULLS LAST) country_length_cume_dist,
        NTILE(3) OVER (PARTITION BY city ORDER BY country_length ASC, id ASC) country_length_bucket
    FROM valid
),
valid_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(running_country_length, country_length_cume_dist) least_running_country_length_country_length_cume_dist,
        UPPER(TRIM(supplier_name)) supplier_name_normalized
    FROM with_windows
    WHERE city IS NOT NULL
)
SELECT
    id,
    country,
    running_country_length,
    country_length_cume_dist,
    country_length_bucket,
    least_running_country_length_country_length_cume_dist,
    supplier_name_normalized,
    CASE WHEN country RLIKE '^(DE|UK)$' THEN 'GROUP_B' WHEN country RLIKE '^(TH|US)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS country_zone
FROM valid_suppliers
ORDER BY least_running_country_length_country_length_cume_dist ASC NULLS FIRST;
