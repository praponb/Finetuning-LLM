WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        team,
        tier,
        hired_on,
        YEAR(hired_on) AS hired_on_year,
        DATE_FORMAT(hired_on, 'yyyy-MM-dd') AS hired_on_label,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM support_agents
    WHERE LOWER(agent_name) LIKE '%ru%'
),
tier_stats AS (
    -- Median hired_on_year per tier
    SELECT tier, PERCENTILE_APPROX(hired_on_year, 0.5) AS median_hired_on_year
    FROM cleaned
    GROUP BY tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_hired_on_year,
        ROUND(s.hired_on_year - g.median_hired_on_year, 2) AS hired_on_year_minus_median
    FROM cleaned s
    JOIN tier_stats g ON g.tier = s.tier
    WHERE s.hired_on_year >= g.median_hired_on_year
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        team,
        tier,
        hired_on,
        hired_on_year,
        tier_or_unknown,
        median_hired_on_year,
        hired_on_year_minus_median,
        median_hired_on_year + hired_on_year AS median_hired_on_year_plus_hired_on_year,
        DATE_FORMAT(hired_on, 'MMM yyyy') AS hired_on_label_2,
        CASE WHEN median_hired_on_year >= 2025.4 THEN 'HIGH' WHEN median_hired_on_year >= 2024.4 THEN 'MEDIUM' ELSE 'LOW' END AS median_hired_on_year_tier
    FROM vs_group
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE hired_on_year_minus_median > (SELECT AVG(hired_on_year_minus_median) FROM tagged)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered
    WHERE median_hired_on_year_plus_hired_on_year < (SELECT MAX(median_hired_on_year_plus_hired_on_year) FROM filtered)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        team,
        tier,
        hired_on,
        hired_on_year,
        tier_or_unknown,
        median_hired_on_year,
        hired_on_year_minus_median,
        median_hired_on_year_plus_hired_on_year,
        hired_on_label_2,
        SUBSTRING(TRIM(hired_on_label_2), 1, 3) AS hired_on_label_2_prefix
    FROM without_matches
)
-- Final projection
SELECT
    id,
    hired_on,
    tier_or_unknown,
    median_hired_on_year,
    hired_on_year_minus_median,
    hired_on_label_2,
    LENGTH(TRIM(hired_on_label_2_prefix)) AS hired_on_label_2_prefix_len,
    CASE WHEN hired_on_year >= 2026 THEN 'A' WHEN hired_on_year >= 2025 THEN 'B' ELSE 'C' END AS hired_on_year_tier
FROM annotated
ORDER BY hired_on_year_minus_median DESC, median_hired_on_year
