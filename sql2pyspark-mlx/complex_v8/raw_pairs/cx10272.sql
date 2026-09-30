WITH valid AS (
    -- Scope the policyholders rows
    SELECT *
    FROM policyholders
    WHERE segment = 'VIP'
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(region, 'UNKNOWN') AS region_or_unknown,
        YEAR(joined_on) AS joined_on_year
    FROM valid
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        joined_on_year,
        region,
        region_or_unknown,
        MAX(joined_on) AS last_joined_on,
        ARRAY_SORT(COLLECT_SET(segment)) AS segment_list
    FROM derived
    GROUP BY joined_on_year, region, region_or_unknown
    HAVING SUM(risk_score) > 0.1
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(last_joined_on) OVER (ORDER BY last_joined_on, joined_on_year, region, region_or_unknown ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_joined_on,
        ROUND(CUME_DIST() OVER w_region_or_unknown_by_last_jo, 3) AS last_joined_on_cume_dist,
        ROUND(joined_on_year / NULLIF(SUM(joined_on_year) OVER w_joined_on_year_region, 0), 4) AS joined_on_year_share_of_joined_on_year_region,
        MAX(joined_on_year) OVER w_joined_on_year_region AS joined_on_year_region_max_joined_on_year
    FROM totals
    WINDOW w_region_or_unknown_by_last_jo AS (PARTITION BY region_or_unknown ORDER BY last_joined_on, joined_on_year, region, region_or_unknown), w_joined_on_year_region AS (PARTITION BY joined_on_year, region)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_last_joined_on, region_or_unknown, joined_on_year_share_of_joined_on_year_region
    FROM sequenced
) src
PIVOT (
    AVG(joined_on_year_share_of_joined_on_year_region)
    FOR region_or_unknown IN ('UNKNOWN', 'Central')
)
