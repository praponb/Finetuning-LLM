WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        CASE WHEN duration_sec >= 352 THEN 'A' WHEN duration_sec >= 209 THEN 'B' ELSE 'C' END AS duration_sec_tier
    FROM tracks
    WHERE release_date IS NOT NULL
),
stats AS (
    -- Group-level totals
    SELECT
        artist_id,
        MIN(duration_sec) AS min_duration_sec,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        SUM(duration_sec) FILTER (WHERE is_explicit = TRUE) AS duration_sec_is_explicit_true,
        COUNT(*) AS row_count,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM cleaned
    GROUP BY artist_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY row_count ORDER BY min_duration_sec) AS min_duration_sec_rank,
        ROUND(distinct_artist_ids / NULLIF(SUM(distinct_artist_ids) OVER (PARTITION BY artist_id, distinct_artist_ids), 0), 4) AS distinct_artist_ids_share_of_artist_id_distinct_,
        NTH_VALUE(distinct_artist_ids, 2) OVER (PARTITION BY row_count ORDER BY min_duration_sec, artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_artist_ids,
        NTH_VALUE(row_count, 2) OVER (PARTITION BY row_count ORDER BY min_duration_sec, artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_row_count,
        ROUND(avg_duration_sec - AVG(avg_duration_sec) OVER (PARTITION BY artist_id, distinct_artist_ids), 2) AS avg_duration_sec_vs_artist_id_distinct_artist_id
    FROM stats
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS last_track_release_date
    FROM with_trends s
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.release_date < DATE '2024-12-22'
        )
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(row_count, 2) OVER (PARTITION BY min_duration_sec_rank, second_row_count ORDER BY min_duration_sec, artist_id) AS next2_row_count,
        SUM(min_duration_sec) OVER (PARTITION BY YEAR(last_track_release_date)) AS last_track_release_date_year_sum_min_duration_se
    FROM qualified
),
annotated AS (
    -- Add calculated columns
    SELECT
        artist_id,
        min_duration_sec,
        distinct_artist_ids,
        row_count,
        avg_duration_sec,
        second_distinct_artist_ids,
        avg_duration_sec_vs_artist_id_distinct_artist_id,
        last_track_release_date,
        last_track_release_date_year_sum_min_duration_se,
        ROUND(avg_duration_sec / NULLIF(distinct_artist_ids, 0), 3) AS avg_duration_sec_per_distinct_artist_ids,
        COALESCE(min_duration_sec, 0) AS min_duration_sec_filled,
        FLOOR(second_row_count / 2) * 2 AS second_row_count_bucket
    FROM sequenced
    WHERE min_duration_sec_rank <= 8
)
-- Final projection
SELECT
    artist_id,
    min_duration_sec,
    distinct_artist_ids,
    row_count,
    avg_duration_sec,
    avg_duration_sec_vs_artist_id_distinct_artist_id,
    last_track_release_date,
    last_track_release_date_year_sum_min_duration_se,
    avg_duration_sec_per_distinct_artist_ids,
    min_duration_sec_filled,
    LEAST(row_count, distinct_artist_ids) AS least_row_count_distinct_artist_ids,
    LEAST(min_duration_sec_filled, avg_duration_sec) AS least_min_duration_sec_filled_avg_duration_sec
FROM annotated
ORDER BY last_track_release_date_year_sum_min_duration_se, artist_id
LIMIT 10
