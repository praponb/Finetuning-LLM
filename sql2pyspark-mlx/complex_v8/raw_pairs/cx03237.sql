WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE is_skipped = TRUE
      AND ms_played >= 97825
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY MONTH(played_at), track_id ORDER BY ms_played DESC, id) AS ms_played_decile,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY listener_id), 0), 4) AS ms_played_share_of_listener_id,
        SUM(ms_played) OVER (ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_sum_2_prior,
        MIN(ms_played) OVER (PARTITION BY listener_id) AS listener_id_min_ms_played
    FROM scoped_plays
),
with_parent_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.ms_played_decile,
        s.ms_played_share_of_listener_id,
        s.ms_played_sum_2_prior,
        s.listener_id_min_ms_played,
        t.release_date AS track_release_date,
        t.title AS track_title,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified
    FROM metrics s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        played_at,
        ms_played_decile,
        ms_played_share_of_listener_id,
        ms_played_sum_2_prior,
        listener_id_min_ms_played,
        track_release_date,
        artist_country,
        artist_is_verified,
        TRUNC(track_release_date, 'MM') AS track_release_date_month_start,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM with_parent_track
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM enriched
    WHERE days_since_played_at < (SELECT MAX(days_since_played_at) FROM enriched)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        played_at,
        ms_played_share_of_listener_id,
        ms_played_sum_2_prior,
        listener_id_min_ms_played,
        track_release_date,
        artist_country,
        artist_is_verified,
        track_release_date_month_start,
        FLOOR(days_since_played_at / 100) * 100 AS days_since_played_at_bucket,
        CASE WHEN ms_played_decile >= 7 THEN 'large' WHEN ms_played_decile >= 4 THEN 'medium' ELSE 'small' END AS ms_played_decile_tier
    FROM without_matches
    WHERE ms_played_sum_2_prior <= 961000
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        played_at,
        ms_played_sum_2_prior,
        listener_id_min_ms_played,
        track_release_date,
        artist_country,
        artist_is_verified,
        track_release_date_month_start,
        ms_played_decile_tier,
        SUM(days_since_played_at_bucket) OVER (PARTITION BY artist_is_verified, ms_played_decile_tier) AS artist_is_verified_ms_played_decile_tier_sum_day,
        SUM(ms_played_sum_2_prior) OVER (PARTITION BY YEAR(track_release_date), artist_country ORDER BY track_release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played_sum_2_prior,
        ROUND(AVG(listener_id_min_ms_played) OVER (PARTITION BY artist_is_verified ORDER BY ms_played_share_of_listener_id ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS listener_id_min_ms_played_avg_6_prior,
        NTILE(5) OVER w_artist_is_verified_by_ms_pla_2 AS ms_played_share_of_listener_id_quintile
    FROM annotated
    WINDOW w_artist_is_verified_by_ms_pla_2 AS (PARTITION BY artist_is_verified ORDER BY ms_played_share_of_listener_id ASC, id)
)
-- Final projection
SELECT
    id,
    ms_played_sum_2_prior,
    listener_id_min_ms_played,
    track_release_date,
    artist_country,
    artist_is_verified,
    artist_is_verified_ms_played_decile_tier_sum_day,
    listener_id_min_ms_played_avg_6_prior
FROM metrics_2
WHERE listener_id_min_ms_played_avg_6_prior > 404500.0
ORDER BY ms_played_sum_2_prior, listener_id_min_ms_played_avg_6_prior DESC
