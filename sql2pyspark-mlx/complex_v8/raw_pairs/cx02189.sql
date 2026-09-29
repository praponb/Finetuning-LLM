WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        YEAR(joined_on) AS joined_on_year,
        SUBSTRING(TRIM(holder_name), 1, 2) AS holder_name_prefix
    FROM policyholders
    WHERE risk_score BETWEEN 0.2 AND 0.7
),
latest AS (
    -- Keep the earliest row per region
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on_year,
        holder_name_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY risk_score, id) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
region_stats AS (
    -- Max joined_on_year per region
    SELECT region, MAX(joined_on_year) AS max_joined_on_year
    FROM latest
    GROUP BY region
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_joined_on_year,
        ROUND(s.joined_on_year - g.max_joined_on_year, 2) AS joined_on_year_minus_max
    FROM latest s
    JOIN region_stats g ON g.region = s.region
    WHERE s.joined_on_year = g.max_joined_on_year
)
SELECT
    id,
    holder_name,
    region,
    risk_score,
    joined_on_year,
    holder_name_prefix,
    max_joined_on_year,
    joined_on_year_minus_max
FROM above_average
ORDER BY holder_name_prefix
