WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm
    FROM tracks
    WHERE (release_date < DATE '2024-12-14' OR artist_id <= 2)
),
latest_per_key AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec, id) AS recency_rank
        FROM valid_tracks
    ) ranked
    WHERE recency_rank = 1
),
combined_details_artist AS (
    -- Enrich with dimension data
    SELECT s.*, a.country AS artist_country
    FROM latest_per_key s
    LEFT JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        FIRST_VALUE(bpm) OVER (PARTITION BY artist_country, artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        NTILE(10) OVER (PARTITION BY artist_country, artist_id ORDER BY bpm DESC, id) AS bpm_decile,
        NTILE(4) OVER (PARTITION BY artist_country, artist_id ORDER BY bpm DESC, id) AS bpm_quartile,
        SUM(duration_sec) OVER (PARTITION BY artist_id, artist_country) AS artist_id_artist_country_sum_duration_sec,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec
    FROM combined_details_artist
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.is_explicit = TRUE) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_is_explicit_count,
        (SELECT ROUND(AVG(t.duration_sec), 2) FROM tracks t WHERE t.artist_id = s.artist_id) AS avg_track_duration_sec
    FROM windowed s
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        first_bpm,
        bpm_decile,
        bpm_quartile,
        artist_id_artist_country_sum_duration_sec,
        track_is_explicit_count,
        LAG(duration_sec, 2, 0) OVER (PARTITION BY track_is_explicit_count, artist_id ORDER BY bpm_quartile NULLS LAST, id) AS prev2_duration_sec,
        LAG(bpm) OVER (PARTITION BY track_is_explicit_count, artist_id ORDER BY bpm_quartile NULLS LAST, id) AS prev_bpm,
        RANK() OVER (PARTITION BY track_is_explicit_count, artist_id ORDER BY bpm_quartile NULLS LAST, id) AS bpm_quartile_rank
    FROM summarized_rows
),
combined_details_artist_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.first_bpm,
        s.bpm_decile,
        s.bpm_quartile,
        s.artist_id_artist_country_sum_duration_sec,
        s.prev2_duration_sec,
        s.bpm_quartile_rank,
        a.artist_name,
        a.country AS artist_country
    FROM sequenced s
    JOIN artists a ON a.id = s.artist_id
    WHERE UPPER(TRIM(a.artist_name)) LIKE 'L%'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, bpm_quartile, bpm_decile, artist_id_artist_country_sum_duration_sec, duration_sec
    FROM combined_details_artist_2
) src
UNPIVOT (
    amount FOR metric IN (bpm_quartile, bpm_decile, artist_id_artist_country_sum_duration_sec, duration_sec)
) unp
