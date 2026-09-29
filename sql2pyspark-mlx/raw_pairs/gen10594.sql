WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        CASE WHEN country = 'SG' THEN 1 ELSE 0 END AS is_sg
    FROM sessions
    WHERE browser IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT country
    FROM valid
    EXCEPT ALL
    SELECT country
    FROM sessions
    WHERE browser IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN matched_keys k ON s.country = k.country
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT device, customer_id
    FROM reconciled
    INTERSECT
    SELECT device, customer_id
    FROM sessions
    WHERE country IN ('JP', 'UK')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    JOIN remaining_keys k ON s.device = k.device AND s.customer_id = k.customer_id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(is_sg) OVER (PARTITION BY country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS is_sg_count_30d,
        AVG(is_sg) OVER (PARTITION BY YEAR(started_at), country) AS started_at_year_avg_is_sg
    FROM kept_rows
    WHERE device = 'mobile'
),
metrics_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(is_sg) OVER (PARTITION BY device, country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_sg_count_7d,
        MIN(is_sg_count_30d) OVER (PARTITION BY YEAR(started_at)) AS started_at_year_min_is_sg_count_30d,
        AVG(started_at_year_avg_is_sg) OVER (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS started_at_year_avg_is_sg_avg_90d
    FROM metrics
)
-- Join a dimension and aggregate
SELECT
    d.segment,
    e.country AS session_country,
    COUNT(*) AS rows_total,
    SUM(e.is_sg_count_7d) AS total_is_sg_count_7d,
    AVG(e.started_at_year_avg_is_sg) AS avg_started_at_year_avg_is_sg,
    GROUPING_ID() AS level_id
FROM metrics_sessions e
JOIN customers d ON e.customer_id = d.id
GROUP BY ROLLUP (d.segment, e.country);
