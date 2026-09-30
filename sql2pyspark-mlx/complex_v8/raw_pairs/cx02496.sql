WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        duration_sec - bpm AS duration_sec_minus_bpm,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        ARRAY_CONTAINS(moods, 'chill') AS has_chill
    FROM tracks
    WHERE release_date < DATE '2025-08-02'
      AND bpm <= 156
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.is_explicit = FALSE) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_is_explicit_count,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count,
        (SELECT MIN(t.bpm) FROM tracks t WHERE t.artist_id = s.artist_id) AS min_track_bpm
    FROM base s
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.duration_sec_minus_bpm,
        s.title_slug,
        s.track_is_explicit_count,
        a.artist_name
    FROM profiled s
    LEFT JOIN artists a ON a.id = s.artist_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        CASE WHEN duration_sec >= 352 THEN 'A' WHEN duration_sec >= 209 THEN 'B' ELSE 'C' END AS duration_sec_tier,
        ROUND(SUM(duration_sec) / NULLIF(SUM(bpm), 0), 3) AS duration_sec_to_bpm_ratio,
        ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list,
        PERCENTILE_APPROX(track_is_explicit_count, 0.5) AS median_track_is_explicit_count,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids
    FROM with_parent_artist
    WHERE UPPER(TRIM(artist_name)) LIKE 'S%'
    GROUP BY CASE WHEN duration_sec >= 352 THEN 'A' WHEN duration_sec >= 209 THEN 'B' ELSE 'C' END
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        duration_sec_tier,
        duration_sec_to_bpm_ratio,
        artist_name_list,
        distinct_artist_ids,
        LAST_VALUE(distinct_artist_ids) OVER (PARTITION BY duration_sec_tier, distinct_artist_ids ORDER BY distinct_artist_ids, duration_sec_tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_artist_ids,
        COUNT(*) OVER w_median_track_is_explicit_cou AS rows_in_median_track_is_explicit_count,
        RANK() OVER (PARTITION BY median_track_is_explicit_count, duration_sec_tier ORDER BY distinct_artist_ids, duration_sec_tier) AS distinct_artist_ids_rank,
        ROW_NUMBER() OVER w_duration_sec_tier_distinct_a_2 AS distinct_artist_ids_seq
    FROM rollup_base
    WINDOW w_median_track_is_explicit_cou AS (PARTITION BY median_track_is_explicit_count), w_duration_sec_tier_distinct_a_2 AS (PARTITION BY duration_sec_tier, distinct_artist_ids ORDER BY distinct_artist_ids, duration_sec_tier)
)
-- Final projection
SELECT
    duration_sec_tier,
    artist_name_list,
    distinct_artist_ids,
    last_distinct_artist_ids,
    rows_in_median_track_is_explicit_count,
    distinct_artist_ids_rank,
    distinct_artist_ids_seq
FROM sequenced
ORDER BY duration_sec_tier
LIMIT 5
