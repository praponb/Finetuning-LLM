WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        LEAST(duration_sec, bpm) AS least_duration_sec_bpm
    FROM tracks
    WHERE SIZE(moods) >= 2
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(played_at) AS last_played_at,
        MAX(ms_played) AS max_play_ms_played,
        COUNT(CASE WHEN is_skipped = FALSE THEN 1 END) AS play_is_skipped_count
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        p.max_play_ms_played,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count
    FROM recent s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
joined_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.genre AS artist_genre
    FROM with_stats s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY least_duration_sec_bpm), 3) AS least_duration_sec_bpm_cume_dist,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY play_is_skipped_count), 2) AS duration_sec_vs_play_is_skipped_count_avg,
        LAG(least_duration_sec_bpm) OVER (PARTITION BY artist_id ORDER BY least_duration_sec_bpm, id) AS prev_least_duration_sec_bpm,
        ROUND(least_duration_sec_bpm / NULLIF(SUM(least_duration_sec_bpm) OVER (PARTITION BY play_is_skipped_count), 0), 4) AS least_duration_sec_bpm_share_of_play_is_skipped_,
        NTILE(4) OVER (PARTITION BY artist_id ORDER BY least_duration_sec_bpm, id) AS least_duration_sec_bpm_quartile
    FROM joined_artist
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.source IN ('search', 'album', 'radio')
        )
),
deduped AS (
    -- Keep the latest row per artist_genre
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        title_prefix,
        least_duration_sec_bpm,
        last_played_at,
        max_play_ms_played,
        play_is_skipped_count,
        artist_genre,
        least_duration_sec_bpm_cume_dist,
        duration_sec_vs_play_is_skipped_count_avg,
        prev_least_duration_sec_bpm,
        least_duration_sec_bpm_share_of_play_is_skipped_,
        least_duration_sec_bpm_quartile
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_genre ORDER BY bpm DESC, id DESC) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        title_prefix,
        least_duration_sec_bpm,
        max_play_ms_played,
        play_is_skipped_count,
        artist_genre,
        duration_sec_vs_play_is_skipped_count_avg,
        prev_least_duration_sec_bpm,
        least_duration_sec_bpm_share_of_play_is_skipped_,
        least_duration_sec_bpm_quartile,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_genre ORDER BY max_play_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(least_duration_sec_bpm) OVER (PARTITION BY artist_id) AS artist_id_sum_least_duration_sec_bpm,
        NTH_VALUE(least_duration_sec_bpm, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_genre ORDER BY max_play_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_least_duration_sec_bpm
    FROM deduped
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    is_explicit,
    bpm,
    title_prefix,
    least_duration_sec_bpm,
    max_play_ms_played,
    play_is_skipped_count,
    artist_genre,
    duration_sec_vs_play_is_skipped_count_avg,
    least_duration_sec_bpm_share_of_play_is_skipped_,
    least_duration_sec_bpm_quartile,
    running_count,
    second_least_duration_sec_bpm,
    CONCAT(artist_genre, '#', CAST(id AS STRING)) AS artist_genre_ref
FROM metrics
ORDER BY artist_genre NULLS LAST, title_prefix NULLS LAST, id
LIMIT 20
