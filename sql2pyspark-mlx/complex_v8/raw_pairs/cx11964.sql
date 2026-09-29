WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        DATE_ADD(release_date, 30) AS release_date_plus_30d
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'M%'
      AND SIZE(moods) >= 1
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        a.genre AS artist_genre,
        a.artist_name
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        artist_genre,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        COUNT(*) FILTER (WHERE UPPER(TRIM(title)) LIKE 'P%') AS n_upper_trim_title_like_p,
        PERCENTILE_APPROX(bpm, 0.5) AS median_bpm,
        COUNT(*) AS row_count
    FROM with_parent_artist
    WHERE bpm < 156
    GROUP BY artist_id, artist_genre
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        artist_id,
        artist_genre,
        title_list,
        median_bpm,
        row_count,
        LAG(median_bpm, 3, 0) OVER w_n_upper_trim_title_like_p_by AS prev3_median_bpm,
        LAG(median_bpm, 2) OVER (PARTITION BY n_upper_trim_title_like_p, artist_id ORDER BY n_upper_trim_title_like_p NULLS LAST, artist_id, artist_genre) AS prev2_median_bpm,
        ROUND(AVG(n_upper_trim_title_like_p) OVER (PARTITION BY n_upper_trim_title_like_p, artist_id ORDER BY n_upper_trim_title_like_p NULLS LAST, artist_id, artist_genre ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS n_upper_trim_title_like_p_avg_7,
        LAST_VALUE(row_count) OVER (PARTITION BY n_upper_trim_title_like_p, artist_id ORDER BY n_upper_trim_title_like_p NULLS LAST, artist_id, artist_genre ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_row_count
    FROM rollup_base
    WINDOW w_n_upper_trim_title_like_p_by AS (PARTITION BY n_upper_trim_title_like_p ORDER BY n_upper_trim_title_like_p, artist_id, artist_genre)
)
SELECT artist_id, artist_genre, metric, amount
FROM (
    SELECT artist_id, artist_genre, row_count, last_row_count, median_bpm
    FROM with_windows
) src
UNPIVOT (
    amount FOR metric IN (row_count, last_row_count, median_bpm)
) unp
