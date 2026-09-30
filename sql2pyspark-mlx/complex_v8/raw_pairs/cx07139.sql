WITH valid_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE (artist_id IS NOT NULL OR release_date < DATE '2025-08-02')
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        COALESCE(bpm, 0) AS bpm_filled,
        ABS(bpm - 127) AS bpm_distance
    FROM valid_tracks
    WHERE ARRAY_CONTAINS(moods, 'party')
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(release_date_week), artist_id ORDER BY bpm DESC, id), 3) AS bpm_cume_dist,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id
    FROM tagged
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(bpm_distance) OVER (PARTITION BY artist_id ORDER BY bpm_filled ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS bpm_distance_avg_2_prior,
        NTILE(5) OVER (ORDER BY bpm_cume_dist, id) AS bpm_cume_dist_quintile,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm_filled ASC NULLS LAST, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY bpm_filled ASC NULLS LAST), 3) AS bpm_filled_percent_rank,
        ROUND(bpm_cume_dist / NULLIF(SUM(bpm_cume_dist) OVER (PARTITION BY artist_id), 0), 4) AS bpm_cume_dist_share_of_artist_id
    FROM sequenced
),
unique_rows AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        release_date_week,
        bpm_filled,
        bpm_distance,
        bpm_cume_dist,
        rows_in_artist_id,
        bpm_distance_avg_2_prior,
        bpm_cume_dist_quintile,
        bpm_vs_artist_id_avg,
        seq_no,
        bpm_filled_percent_rank,
        bpm_cume_dist_share_of_artist_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm_cume_dist_share_of_artist_id DESC, id DESC) AS rn
        FROM windowed
    ) ranked
    WHERE rn = 1
),
artist_id_stats AS (
    -- Avg bpm_distance per artist_id
    SELECT artist_id, AVG(bpm_distance) AS avg_bpm_distance
    FROM unique_rows
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_bpm_distance,
        ROUND(s.bpm_distance - g.avg_bpm_distance, 2) AS bpm_distance_minus_avg
    FROM unique_rows s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm_distance > g.avg_bpm_distance
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM outliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.release_date >= DATE '2024-06-03'
              AND t.duration_sec < 402
        )
)
SELECT
    id,
    artist_id,
    title,
    bpm,
    moods,
    release_date_week,
    bpm_filled,
    bpm_distance,
    bpm_cume_dist,
    rows_in_artist_id,
    bpm_cume_dist_quintile,
    bpm_vs_artist_id_avg,
    seq_no,
    bpm_cume_dist_share_of_artist_id,
    avg_bpm_distance
FROM screened
WHERE bpm >= 96
ORDER BY bpm_cume_dist_quintile, avg_bpm_distance
