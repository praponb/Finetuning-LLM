WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'N%'
      AND is_explicit = FALSE
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        release_date_week,
        title_len,
        ROUND(title_len - AVG(title_len) OVER (PARTITION BY title_len, is_explicit), 2) AS title_len_vs_title_len_is_explicit_avg,
        MAX(title_len) OVER (PARTITION BY artist_id, title_len ORDER BY bpm, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS title_len_max_1_prior,
        SIZE(COLLECT_SET(artist_id) OVER w_title_len_is_explicit) AS distinct_artist_id_in_title_len_is_explicit,
        MIN(title_len) OVER (PARTITION BY title_len, is_explicit) AS title_len_is_explicit_min_title_len,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, title_len ORDER BY bpm), 3) AS bpm_percent_rank,
        ROUND((UNIX_TIMESTAMP(release_date_week) - UNIX_TIMESTAMP(LAG(release_date_week) OVER (PARTITION BY artist_id, title_len ORDER BY bpm, id))) / 60, 1) AS minutes_since_prev_release_date_week
    FROM scoped_tracks
    WINDOW w_title_len_is_explicit AS (PARTITION BY title_len, is_explicit)
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, a.is_verified AS artist_is_verified
    FROM windowed s
    JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    bpm,
    moods,
    title_len_vs_title_len_is_explicit_avg,
    title_len_max_1_prior,
    distinct_artist_id_in_title_len_is_explicit,
    title_len_is_explicit_min_title_len,
    minutes_since_prev_release_date_week,
    artist_is_verified,
    CASE WHEN title_len_max_1_prior >= 26 THEN 'A' WHEN title_len_max_1_prior >= 18 THEN 'B' ELSE 'C' END AS title_len_max_1_prior_tier
FROM joined
ORDER BY release_date, id
