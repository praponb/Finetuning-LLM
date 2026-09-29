WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        ADD_MONTHS(release_date, 6) AS release_date_plus_6m
    FROM tracks
    WHERE LOWER(title) LIKE '%ci%'
      AND bpm >= 75
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        title,
        bpm,
        moods,
        SIZE(COLLECT_SET(artist_id) OVER w_release_date_plus_6m_year) AS distinct_artist_id_in_release_date_plus_6m_year,
        DENSE_RANK() OVER (PARTITION BY YEAR(release_date_plus_6m) ORDER BY release_date_plus_6m) AS release_date_plus_6m_dense_rank,
        SUM(bpm) OVER w_release_date_plus_6m_year AS release_date_plus_6m_year_sum_bpm,
        MAX(duration_sec) OVER (PARTITION BY YEAR(release_date_plus_6m)) AS release_date_plus_6m_year_max_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY YEAR(release_date_plus_6m) ORDER BY release_date_plus_6m, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_max_7
    FROM base
    WINDOW w_release_date_plus_6m_year AS (PARTITION BY YEAR(release_date_plus_6m))
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM sequenced s
    WHERE s.id IN (
            SELECT p.track_id
            FROM plays p
        )
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
        SIZE(moods) AS moods_count,
        GREATEST(duration_sec_max_7, bpm) AS greatest_duration_sec_max_7_bpm
    FROM qualified
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.title,
        s.bpm,
        s.distinct_artist_id_in_release_date_plus_6m_year,
        s.release_date_plus_6m_dense_rank,
        s.release_date_plus_6m_year_sum_bpm,
        s.release_date_plus_6m_year_max_duration_sec,
        s.duration_sec_max_7,
        s.moods_text,
        s.moods_count,
        s.greatest_duration_sec_max_7_bpm,
        t.mood
    FROM derived s
    LATERAL VIEW OUTER EXPLODE(s.moods) t AS mood
),
grouped AS (
    -- Summarise per group
    SELECT
        moods_count,
        mood,
        ROUND(STDDEV(moods_count), 2) AS stddev_moods_count,
        SUM(greatest_duration_sec_max_7_bpm) AS total_greatest_duration_sec_max_7_bpm,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        SUM(release_date_plus_6m_year_max_duration_sec) AS total_release_date_plus_6m_year_max_duration_sec,
        MIN(distinct_artist_id_in_release_date_plus_6m_year) AS min_distinct_artist_id_in_release_date_plus_6m_y
    FROM per_label
    GROUP BY moods_count, mood
    HAVING SUM(release_date_plus_6m_dense_rank) > 4
),
tagged AS (
    -- Derive helper columns
    SELECT
        moods_count,
        mood,
        stddev_moods_count,
        total_greatest_duration_sec_max_7_bpm,
        title_list,
        total_release_date_plus_6m_year_max_duration_sec,
        LEAST(moods_count, stddev_moods_count) AS least_moods_count_stddev_moods_count,
        ROUND(stddev_moods_count * 0.9, 2) AS stddev_moods_count_adj
    FROM grouped
)
-- Final projection
SELECT
    moods_count,
    mood,
    stddev_moods_count,
    total_greatest_duration_sec_max_7_bpm,
    title_list,
    least_moods_count_stddev_moods_count,
    stddev_moods_count_adj
FROM tagged
ORDER BY stddev_moods_count DESC, mood DESC
