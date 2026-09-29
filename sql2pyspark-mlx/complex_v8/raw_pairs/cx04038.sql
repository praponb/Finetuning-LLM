WITH scoped AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE bpm < 166
      AND NOT is_explicit
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        moods,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        MAX(duration_sec) OVER w_artist_id AS artist_id_max_duration_sec,
        NTH_VALUE(release_date, 2) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        RANK() OVER (ORDER BY bpm DESC) AS bpm_rank
    FROM scoped
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        moods,
        bpm_vs_artist_id_avg,
        artist_id_max_duration_sec,
        second_release_date
    FROM sequenced
    WHERE bpm_rank <= 3
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        COUNT(*) FILTER (WHERE LOWER(title) LIKE '%bu%') AS n_lower_title_like_bu,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(artist_id_max_duration_sec), 2) AS avg_artist_id_max_duration_sec,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS n_is_explicit_true
    FROM first_rows
    GROUP BY DATE_TRUNC('MONTH', release_date)
)
SELECT
    release_date_month,
    n_lower_title_like_bu,
    distinct_ids,
    avg_artist_id_max_duration_sec,
    n_is_explicit_true,
    ABS(distinct_ids - 6) AS distinct_ids_distance,
    CASE WHEN n_lower_title_like_bu >= 8 THEN 'HIGH' WHEN n_lower_title_like_bu >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS n_lower_title_like_bu_tier
FROM grouped
WHERE row_count >= 6
ORDER BY n_lower_title_like_bu DESC
