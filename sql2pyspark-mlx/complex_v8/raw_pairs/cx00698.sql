WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
),
with_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_bucket,
        l.country AS listener_country,
        l.plan AS listener_plan,
        l.birth_year AS listener_birth_year,
        t.bpm AS track_bpm,
        t.is_explicit AS track_is_explicit
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
    WHERE l.birth_year > 1975
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        listener_country,
        listener_plan,
        track_bpm,
        ROUND(AVG(ms_played_bucket) OVER (PARTITION BY listener_plan, source), 2) AS listener_plan_source_avg_ms_played_bucket,
        LAG(ms_played, 2) OVER w_track_is_explicit_device_by AS prev2_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id), 2) AS track_id_avg_ms_played,
        LAG(ms_played_bucket, 2, 0) OVER (PARTITION BY listener_country, device ORDER BY listener_birth_year DESC, id) AS prev2_ms_played_bucket,
        ROUND(listener_birth_year - AVG(listener_birth_year) OVER (PARTITION BY track_id), 2) AS listener_birth_year_vs_track_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_country ORDER BY ms_played_bucket DESC NULLS FIRST, id), 3) AS ms_played_bucket_cume_dist,
        ROUND(AVG(track_bpm) OVER (PARTITION BY listener_plan, source), 2) AS listener_plan_source_avg_track_bpm,
        LAST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY listener_country, device ORDER BY listener_birth_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at
    FROM with_details_listener
    WINDOW w_track_is_explicit_device_by AS (PARTITION BY track_is_explicit, device ORDER BY ms_played ASC, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(listener_country, '#', CAST(listener_id AS STRING)) AS listener_country_ref
    FROM ranked
    WHERE HOUR(last_known_played_at) BETWEEN 6 AND 10
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        device,
        listener_country,
        ROUND(SUM(ms_played_bucket_cume_dist) / NULLIF(SUM(ms_played), 0), 3) AS ms_played_bucket_cume_dist_to_ms_played_ratio,
        MAX(listener_plan_source_avg_track_bpm) AS max_listener_plan_source_avg_track_bpm,
        ROUND(100 * COUNT(CASE WHEN listener_country <> 'BR' THEN 1 END) / COUNT(*), 1) AS pct_listener_country_br
    FROM tagged
    WHERE listener_birth_year_vs_track_id_avg BETWEEN -1405.6 AND 803.2
    GROUP BY 1, 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        device,
        listener_country,
        max_listener_plan_source_avg_track_bpm,
        pct_listener_country_br,
        ROUND(AVG(pct_listener_country_br) OVER (PARTITION BY device ORDER BY pct_listener_country_br NULLS LAST, device, listener_country ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS pct_listener_country_br_avg_7,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        ROUND(AVG(pct_listener_country_br) OVER w_device, 2) AS device_avg_pct_listener_country_br
    FROM grouped
    WINDOW w_device AS (PARTITION BY device)
)
-- Final projection
SELECT
    device,
    listener_country,
    max_listener_plan_source_avg_track_bpm,
    pct_listener_country_br,
    pct_listener_country_br_avg_7,
    rows_in_device,
    device_avg_pct_listener_country_br
FROM with_windows
ORDER BY max_listener_plan_source_avg_track_bpm, device NULLS LAST, listener_country
LIMIT 10
