WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE bpm >= 76
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ABS(bpm - 143) AS bpm_distance,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label,
        CONCAT(is_explicit_label, '#', CAST(artist_id AS STRING)) AS is_explicit_label_ref
    FROM filtered
    WHERE ARRAY_CONTAINS(moods, 'chill')
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(is_explicit_label_ref)) AS is_explicit_label_ref_len,
        FLOOR(bpm_distance / 25) * 25 AS bpm_distance_bucket
    FROM enriched
),
artist_id_stats AS (
    -- Avg bpm_distance_bucket per artist_id
    SELECT artist_id, AVG(bpm_distance_bucket) AS avg_bpm_distance_bucket
    FROM labeled
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_bpm_distance_bucket,
        ROUND(s.bpm_distance_bucket - g.avg_bpm_distance_bucket, 2) AS bpm_distance_bucket_minus_avg
    FROM labeled s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm_distance_bucket > g.avg_bpm_distance_bucket
)
SELECT id, measure, amount
FROM (
    SELECT id, bpm_distance_bucket_minus_avg, avg_bpm_distance_bucket
    FROM above_average
) src
UNPIVOT (
    amount FOR measure IN (bpm_distance_bucket_minus_avg, avg_bpm_distance_bucket)
) unp
ORDER BY id, measure
