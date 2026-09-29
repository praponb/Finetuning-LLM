WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        LENGTH(TRIM(title)) AS title_len,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE (bpm < 166 OR NOT ARRAY_CONTAINS(moods, 'chill'))
),
joined_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.title_len,
        s.release_date_month,
        a.artist_name
    FROM prepared_tracks s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        title_len,
        artist_name,
        MIN(duration_sec) OVER (PARTITION BY title_len ORDER BY release_date_month DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        ROUND(AVG(duration_sec) OVER (PARTITION BY title_len ORDER BY release_date_month DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_6_prior,
        COUNT(*) OVER (PARTITION BY YEAR(release_date)) AS rows_in_release_date_year,
        ROUND(CUME_DIST() OVER (PARTITION BY title_len ORDER BY release_date_month DESC NULLS FIRST, id), 3) AS release_date_month_cume_dist,
        DENSE_RANK() OVER w_title_len_by_release_date_mo_3 AS release_date_month_dense_rank
    FROM joined_artist
    WINDOW w_title_len_by_release_date_mo_3 AS (PARTITION BY title_len ORDER BY release_date_month DESC NULLS FIRST, id)
),
latest_per_key AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        title_len,
        artist_name,
        min_duration_sec_so_far,
        duration_sec_avg_6_prior,
        rows_in_release_date_year,
        release_date_month_cume_dist,
        release_date_month_dense_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date_month_cume_dist, id) AS row_num
        FROM windowed
    ) ranked
    WHERE row_num = 1
)
SELECT
    artist_id,
    release_date_month_dense_rank,
    GROUPING(artist_id) AS is_total_artist_id,
    ROUND(AVG(release_date_month_cume_dist), 2) AS avg_release_date_month_cume_dist
FROM latest_per_key
GROUP BY CUBE(artist_id, release_date_month_dense_rank)
ORDER BY artist_id NULLS LAST, release_date_month_dense_rank NULLS LAST
