WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ABS(bpm - 104) AS bpm_distance
    FROM tracks
    WHERE SIZE(moods) >= 2
      AND duration_sec > 213
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        FLOOR(bpm_distance / 25) * 25 AS bpm_distance_bucket
    FROM filtered_tracks
    WHERE bpm_distance <= 162
),
unique_rows AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        bpm_distance_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm_distance_bucket DESC, id DESC) AS recency_rank
        FROM annotated
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        release_date,
        moods,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY release_date DESC), 3) AS release_date_cume_dist,
        ROUND(bpm_distance_bucket - AVG(bpm_distance_bucket) OVER (PARTITION BY artist_id, is_explicit), 2) AS bpm_distance_bucket_vs_artist_id_is_explicit_avg,
        NTILE(4) OVER w_artist_id_by_release_date AS release_date_quartile,
        SUM(bpm_distance_bucket) OVER w_artist_id_is_explicit AS artist_id_is_explicit_sum_bpm_distance_bucket,
        NTH_VALUE(release_date, 2) OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        DENSE_RANK() OVER (ORDER BY release_date) AS release_date_dense_rank
    FROM unique_rows
    WINDOW w_artist_id_by_release_date AS (PARTITION BY artist_id ORDER BY release_date DESC, id), w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit)
),
first_rows AS (
    -- Top 2 per group
    SELECT
        id,
        artist_id,
        release_date,
        moods,
        release_date_cume_dist,
        bpm_distance_bucket_vs_artist_id_is_explicit_avg,
        release_date_quartile,
        artist_id_is_explicit_sum_bpm_distance_bucket,
        second_release_date
    FROM ranked
    WHERE release_date_dense_rank <= 2
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM first_rows s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
        )
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', second_release_date) AS days_since_second_release_date,
        DATE_ADD(release_date, 30) AS release_date_plus_30d,
        DATEDIFF(second_release_date, release_date) AS days_release_date_to_second_release_date
    FROM without_matches
    WHERE bpm_distance_bucket_vs_artist_id_is_explicit_avg <= 126.0
)
SELECT
    id,
    release_date_cume_dist,
    bpm_distance_bucket_vs_artist_id_is_explicit_avg,
    artist_id_is_explicit_sum_bpm_distance_bucket,
    second_release_date,
    release_date_plus_30d,
    days_release_date_to_second_release_date
FROM derived
ORDER BY release_date_cume_dist, days_release_date_to_second_release_date
