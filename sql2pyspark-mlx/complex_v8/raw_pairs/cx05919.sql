WITH filtered_listeners AS (
    -- Filter listeners
    SELECT
        id,
        country,
        signup_date,
        interests,
        CONCAT(country, '#', CAST(birth_year AS STRING)) AS country_ref,
        CASE WHEN birth_year >= 1994 THEN 'HIGH' WHEN birth_year >= 1975 THEN 'MEDIUM' ELSE 'LOW' END AS birth_year_tier,
        CASE WHEN birth_year >= 1988 THEN 'A' WHEN birth_year >= 1975 THEN 'B' ELSE 'C' END AS birth_year_tier_2
    FROM listeners
    WHERE plan IN ('family', 'premium')
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(birth_year_tier_2), 1, 3) AS birth_year_tier_2_prefix,
        DATE_FORMAT(signup_date, 'MMM yyyy') AS signup_date_label,
        LAST_DAY(signup_date) AS signup_date_month_end
    FROM filtered_listeners
)
-- Final projection
SELECT
    id,
    country,
    signup_date,
    interests,
    country_ref,
    birth_year_tier,
    birth_year_tier_2_prefix,
    signup_date_label,
    signup_date_month_end
FROM enriched
ORDER BY birth_year_tier DESC
