WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND duration_sec > 144
),
combined_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.country AS artist_country
    FROM recent s
    JOIN artists a ON a.id = s.artist_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        DENSE_RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm DESC, id) AS bpm_dense_rank,
        NTH_VALUE(release_date, 2) OVER (ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        COUNT(*) OVER (PARTITION BY YEAR(release_date)) AS rows_in_release_date_year,
        ROUND(AVG(bpm) OVER (ORDER BY release_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_7,
        NTH_VALUE(bpm, 2) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        SUM(bpm) OVER (ORDER BY release_date DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS bpm_sum_7,
        ROW_NUMBER() OVER w_all_by_bpm AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm DESC, id), 3) AS bpm_cume_dist,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY YEAR(release_date))) AS distinct_artist_id_in_release_date_year
    FROM combined_details
    WINDOW w_all_by_bpm AS (ORDER BY bpm, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        bpm_dense_rank,
        second_release_date,
        rows_in_release_date_year,
        bpm_avg_7,
        second_bpm,
        bpm_sum_7,
        bpm_cume_dist,
        distinct_artist_id_in_release_date_year
    FROM with_trends
    WHERE rn <= 3
)
SELECT
    id,
    artist_id,
    release_date,
    is_explicit,
    bpm,
    moods,
    second_release_date,
    bpm_avg_7,
    second_bpm,
    bpm_sum_7,
    bpm_cume_dist,
    distinct_artist_id_in_release_date_year,
    IF(is_explicit, 'yes', 'no') AS is_explicit_label,
    DATEDIFF(DATE '2026-01-01', second_release_date) AS days_since_second_release_date
FROM top_per_group
WHERE artist_id IS NOT NULL
ORDER BY is_explicit_label DESC, bpm_cume_dist NULLS LAST
