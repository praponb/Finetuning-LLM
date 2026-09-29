WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        bpm + duration_sec AS bpm_plus_duration_sec,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE duration_sec < 353
),
joined_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.bpm_plus_duration_sec,
        s.duration_sec_bucket,
        s.bpm_bucket,
        a.debut_year AS artist_debut_year
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(duration_sec_bucket, 2) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_bucket,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_6_prior,
        ROUND(AVG(bpm_plus_duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS bpm_plus_duration_sec_avg_1_prior
    FROM joined_artist
),
latest_per_key AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        bpm_plus_duration_sec,
        duration_sec_bucket,
        bpm_bucket,
        artist_debut_year,
        second_duration_sec_bucket,
        bpm_avg_6_prior,
        bpm_plus_duration_sec_avg_1_prior
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_debut_year DESC, id DESC) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(duration_sec_bucket - 206) AS duration_sec_bucket_distance,
        LENGTH(TRIM(title)) AS title_len,
        GREATEST(duration_sec_bucket, artist_debut_year) AS greatest_duration_sec_bucket_artist_debut_year
    FROM latest_per_key
)
-- Subtotals with ROLLUP
SELECT
    title_len,
    is_explicit,
    PERCENTILE_APPROX(bpm_plus_duration_sec_avg_1_prior, 0.5) AS median_bpm_plus_duration_sec_avg_1_prior,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids
FROM tagged
GROUP BY ROLLUP(title_len, is_explicit)
