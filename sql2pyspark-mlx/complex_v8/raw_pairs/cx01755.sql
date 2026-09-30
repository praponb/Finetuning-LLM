WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE source IN ('search', 'album', 'radio')
      AND MONTH(played_at) IN (1, 2)
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS first_played_at
    FROM valid s
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        is_skipped,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        CASE WHEN ms_played >= 245877 THEN 'HIGH' WHEN ms_played >= 145720 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        MAX(ms_played) AS max_ms_played,
        MIN(ms_played) AS min_ms_played
    FROM profiled
    GROUP BY is_skipped, DATE_TRUNC('MONTH', played_at), CASE WHEN ms_played >= 245877 THEN 'HIGH' WHEN ms_played >= 145720 THEN 'MEDIUM' ELSE 'LOW' END
    HAVING AVG(ms_played) > 92277
),
enriched AS (
    -- Add calculated columns
    SELECT
        is_skipped,
        played_at_month,
        ms_played_tier,
        min_ms_played,
        LENGTH(TRIM(ms_played_tier)) AS ms_played_tier_len
    FROM rollup_base
)
SELECT
    ms_played_tier_len,
    COALESCE(ms_played_tier, 'ALL') AS ms_played_tier,
    SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true,
    MIN(min_ms_played) AS min_min_ms_played
FROM enriched
GROUP BY GROUPING SETS ((ms_played_tier_len, ms_played_tier), (ms_played_tier_len))
ORDER BY ms_played_tier_len NULLS LAST, ms_played_tier NULLS LAST
