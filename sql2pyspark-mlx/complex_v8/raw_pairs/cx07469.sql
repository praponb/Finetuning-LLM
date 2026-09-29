WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-18 12:00:00'
),
listener_id_stats AS (
    -- Max ms_played per listener_id
    SELECT listener_id, MAX(ms_played) AS max_ms_played
    FROM recent_plays
    GROUP BY listener_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ms_played,
        ROUND(s.ms_played - g.max_ms_played, 2) AS ms_played_minus_max
    FROM recent_plays s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played = g.max_ms_played
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        is_skipped,
        max_ms_played,
        ms_played_minus_max,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped ORDER BY max_ms_played ASC, id), 3) AS max_ms_played_percent_rank,
        ROUND(ms_played_minus_max / NULLIF(SUM(ms_played_minus_max) OVER (PARTITION BY track_id, listener_id), 0), 4) AS ms_played_minus_max_share_of_track_id_listener_i,
        ROUND(CUME_DIST() OVER w_is_skipped_by_max_ms_played_2, 3) AS max_ms_played_cume_dist
    FROM outliers
    WINDOW w_is_skipped_by_max_ms_played_2 AS (PARTITION BY is_skipped ORDER BY max_ms_played ASC)
),
with_parent AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.ms_played_minus_max,
        s.max_ms_played_percent_rank,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year,
        l.plan AS listener_plan
    FROM windowed s
    JOIN listeners l ON l.id = s.listener_id
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        l.country AS listener_country,
        l.birth_year AS listener_birth_year_2,
        l.username AS listener_username
    FROM with_parent s
    JOIN listeners l ON l.id = s.listener_id
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_details s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE p.track_id IS NOT NULL
              AND p.source = 'radio'
        )
),
summary AS (
    -- Group-level totals
    SELECT
        listener_country,
        is_skipped,
        DATE_FORMAT(listener_signup_date, 'yyyy-MM') AS listener_signup_date_ym,
        SUM(ms_played) AS total_ms_played,
        COUNT(*) AS cnt,
        SUM(listener_birth_year) AS total_listener_birth_year,
        COUNT(DISTINCT listener_plan) AS distinct_listener_plans,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played,
        SUM(listener_birth_year_2) AS total_listener_birth_year_2
    FROM filtered
    GROUP BY listener_country, is_skipped, DATE_FORMAT(listener_signup_date, 'yyyy-MM')
)
-- Final projection
SELECT
    listener_country,
    is_skipped,
    listener_signup_date_ym,
    total_ms_played,
    cnt,
    total_listener_birth_year,
    distinct_listener_plans,
    median_ms_played,
    COALESCE(listener_country, 'UNKNOWN') AS listener_country_or_unknown
FROM summary
ORDER BY listener_signup_date_ym NULLS LAST
