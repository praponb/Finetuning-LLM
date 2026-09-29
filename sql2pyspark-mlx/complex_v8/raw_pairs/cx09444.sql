WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        CONCAT_WS('|', source, device) AS source_device_label,
        UPPER(TRIM(source)) AS source_clean
    FROM plays
    WHERE (listener_id IS NOT NULL OR ms_played >= 120663)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_ms_played, 3) AS ms_played_percent_rank,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_listener_id_track_id, 0), 4) AS ms_played_share_of_listener_id_track_id,
        RANK() OVER (ORDER BY ms_played) AS ms_played_rank,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id, track_id), 2) AS ms_played_vs_listener_id_track_id_avg
    FROM filtered_plays
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played, id), w_listener_id_track_id AS (PARTITION BY listener_id, track_id)
),
enriched_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source_clean,
        s.ms_played_percent_rank,
        s.ms_played_rank,
        l.country AS listener_country,
        l.signup_date AS listener_signup_date,
        l.username AS listener_username,
        t.duration_sec AS track_duration_sec,
        t.bpm AS track_bpm
    FROM metrics s
    JOIN listeners l ON l.id = s.listener_id AND l.plan = 'family'
    LEFT JOIN tracks t ON t.id = s.track_id
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(listener_signup_date, 'yyyy-MM') AS listener_signup_date_ym,
        SUM(ms_played) FILTER (WHERE LENGTH(TRIM(listener_username)) >= 7) AS ms_played_length_trim_listener_usernam,
        SUM(CASE WHEN UPPER(TRIM(listener_username)) LIKE 'G%' THEN 1 ELSE 0 END) AS n_upper_trim_listener_username,
        SUM(track_duration_sec) AS total_track_duration_sec,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played,
        MIN(listener_signup_date) AS first_listener_signup_date,
        MAX(played_at) AS last_played_at
    FROM enriched_listener
    GROUP BY DATE_FORMAT(listener_signup_date, 'yyyy-MM')
),
scored AS (
    -- Label and score rows
    SELECT
        listener_signup_date_ym,
        ms_played_length_trim_listener_usernam,
        total_track_duration_sec,
        last_played_at,
        LOWER(TRIM(listener_signup_date_ym)) AS listener_signup_date_ym_clean,
        CASE WHEN median_ms_played >= 281500 THEN 'large' WHEN median_ms_played >= 103750 THEN 'medium' ELSE 'small' END AS median_ms_played_tier,
        DATEDIFF(DATE '2025-12-31', last_played_at) AS days_since_last_played_at
    FROM stats
    WHERE total_track_duration_sec <= 2034
)
-- Final projection
SELECT
    listener_signup_date_ym,
    ms_played_length_trim_listener_usernam,
    total_track_duration_sec,
    last_played_at,
    listener_signup_date_ym_clean,
    median_ms_played_tier,
    days_since_last_played_at,
    CONCAT(median_ms_played_tier, '#', CAST(total_track_duration_sec AS STRING)) AS median_ms_played_tier_ref,
    LOWER(REGEXP_REPLACE(TRIM(median_ms_played_tier), ' ', '_')) AS median_ms_played_tier_slug
FROM scored
ORDER BY median_ms_played_tier DESC
